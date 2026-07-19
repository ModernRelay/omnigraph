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
    /// After Lance returns success from its two-phase native create, before
    /// OmniGraph acknowledges it. Recovery must classify the matching
    /// BranchContents as a completed create (lost acknowledgement).
    pub const BRANCH_CREATE_POST_NATIVE: &str = "branch_create.post_native";
    pub const BRANCH_DELETE_BEFORE_TABLE_CLEANUP: &str = "branch_delete.before_table_cleanup";
    /// After Lance returns success from native delete, before OmniGraph
    /// acknowledges it. Recovery must classify the absent BranchContents as a
    /// completed logical deletion.
    pub const BRANCH_DELETE_POST_NATIVE: &str = "branch_delete.post_native";
    /// Branch delete holds the schema, target-branch, and fresh-catalog table
    /// envelope and has completed its final recovery check, before the native
    /// manifest-ref mutation.
    pub const BRANCH_DELETE_POST_TABLE_GATES: &str = "branch_delete.post_table_gates";
    /// After native branch control completed its first recovery barrier, before
    /// it acquires schema -> branch -> table gates and performs the final check.
    pub const BRANCH_CONTROL_POST_RECOVERY_BARRIER: &str = "branch_control.post_recovery_barrier";
    pub const BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT: &str =
        "branch_merge.adopt_after_append_pre_upsert";
    /// After one bounded strict-insert chunk committed while at least one later
    /// chunk from the same Armed BranchMerge transaction chain remains.
    pub const BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS: &str =
        "branch_merge.adopt_between_insert_chunks";
    pub const BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE: &str =
        "branch_merge.adopt_after_upsert_pre_delete";
    /// After one bounded delete chunk committed while at least one later
    /// delete chunk from the same Armed BranchMerge chain remains.
    pub const BRANCH_MERGE_BETWEEN_DELETE_CHUNKS: &str = "branch_merge.between_delete_chunks";
    /// Source/target heads and snapshots have been captured while the schema
    /// and both branch-incarnation gates are held, before merge planning or
    /// any durable table effect.
    pub const BRANCH_MERGE_POST_AUTHORITY_CAPTURE: &str = "branch_merge.post_authority_capture";
    /// Candidate classification and validation have completed, before the
    /// final source/target table-gate envelope and recovery arm. Tests use this
    /// boundary to prove a raw source-table ref delete/recreate cannot pass as
    /// the native incarnation whose immutable rows were proven.
    pub const BRANCH_MERGE_POST_CANDIDATE_VALIDATION: &str =
        "branch_merge.post_candidate_validation";
    /// The v4 BranchMerge recovery intent is durable, before any first-touch
    /// target table ref is created.
    pub const BRANCH_MERGE_POST_SIDECAR_PRE_FORK: &str = "branch_merge.post_sidecar_pre_fork";
    pub const BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT: &str =
        "branch_merge.post_phase_b_pre_manifest_commit";
    /// Every merge table effect is complete, but the sidecar is still in its
    /// pre-confirmation shape.
    pub const BRANCH_MERGE_POST_EFFECTS_PRE_CONFIRM: &str = "branch_merge.post_effects_pre_confirm";
    pub const BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_INDEX: &str =
        "branch_merge.rewrite_after_delete_pre_index";
    pub const BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE: &str =
        "branch_merge.rewrite_after_merge_pre_delete";
    pub const CLASSIFY_FRESH_READ: &str = "classify.fresh_read";
    pub const CLEANUP_RECONCILE_FORK: &str = "cleanup.reconcile_fork";
    /// After cleanup's fast empty-sidecar probe, before it acquires the closed
    /// schema/branch/table GC gate set and performs the authoritative recheck.
    pub const CLEANUP_POST_RECOVERY_CHECK_PRE_GATES: &str = "cleanup.post_recovery_check_pre_gates";
    pub const CLEANUP_RESOLVE_BRANCH_SNAPSHOT: &str = "cleanup.resolve_branch_snapshot";
    pub const CLEANUP_TABLE_GC: &str = "cleanup.table_gc";
    pub const ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT: &str =
        "ensure_indices.post_phase_b_pre_manifest_commit";
    /// Every exact index transaction and first-touch ref effect is durable,
    /// but the v8 sidecar is still Armed. Recovery must therefore compensate
    /// rather than infer the intended manifest delta from physical state.
    pub const ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM: &str =
        "ensure_indices.post_effects_pre_confirm";
    pub const ENSURE_INDICES_POST_SIDECAR_PRE_FORK: &str = "ensure_indices.post_sidecar_pre_fork";
    pub const ENSURE_INDICES_POST_TABLE_EFFECT: &str = "ensure_indices.post_table_effect";
    pub const ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE: &str =
        "ensure_indices.post_stage_pre_commit_btree";
    pub const FORK_BEFORE_CLASSIFY: &str = "fork.before_classify";
    pub const FORK_BEFORE_RECLAIM: &str = "fork.before_reclaim";
    /// After Lance durably creates a target table ref, before the caller can
    /// reopen and verify it. An error here is post-effect and must retain the
    /// recovery sidecar.
    pub const FORK_POST_CREATE_PRE_OPEN: &str = "fork.post_create_pre_open";
    pub const GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT: &str = "graph_publish.after_manifest_commit";
    pub const GRAPH_PUBLISH_BEFORE_COMMIT_APPEND: &str = "graph_publish.before_commit_append";
    pub const INIT_AFTER_COORDINATOR_INIT: &str = "init.after_coordinator_init";
    pub const INIT_AFTER_SCHEMA_CONTRACT_WRITTEN: &str = "init.after_schema_contract_written";
    pub const INIT_AFTER_SCHEMA_PG_WRITTEN: &str = "init.after_schema_pg_written";
    pub const MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE: &str =
        "mutation.delete_node_pre_primary_delete";
    /// After every deferred first-touch table ref is created under a durable
    /// v3 sidecar, before any staged data transaction advances target HEAD.
    pub const MUTATION_POST_FORK_PRE_COMMIT: &str = "mutation.post_fork_pre_commit";
    /// After each exact staged table transaction advances HEAD, before the next
    /// table effect or Phase-B confirmation. Used to leave a real partial
    /// multi-table v3 attempt whose remaining first-touch fork still needs
    /// recovery cleanup.
    pub const MUTATION_POST_TABLE_COMMIT: &str = "mutation.post_table_commit";
    /// After the v3 ownership sidecar is durable but before the first deferred
    /// named-table ref is created. Recovery must accept the absent target ref.
    pub const MUTATION_POST_SIDECAR_PRE_FORK: &str = "mutation.post_sidecar_pre_fork";
    /// Deterministic OCC rendezvous after a mutation has validated and staged
    /// its complete attempt, but before the RFC-022 branch effect gate is
    /// acquired and the write authority token is revalidated. Tests park the
    /// first writer here, commit a conflicting second writer, then prove the
    /// first attempt is discarded and validation is rerun from a fresh token.
    pub const MUTATION_POST_STAGE_PRE_EFFECT_GATE: &str = "mutation.post_stage_pre_effect_gate";
    pub const MUTATION_POST_FINALIZE_PRE_PUBLISHER: &str = "mutation.post_finalize_pre_publisher";
    /// Open owns the schema gate and is about to read source/IR/state as one
    /// catalog view.
    pub const OPEN_BEFORE_SCHEMA_CONTRACT_READ: &str = "open.before_schema_contract_read";
    pub const OPTIMIZE_BEFORE_COMPACT: &str = "optimize.before_compact";
    pub const OPTIMIZE_INJECT_REINDEX_CONFLICT: &str = "optimize.inject_reindex_conflict";
    /// After Optimize's broad recovery fast-path check, before the main-branch
    /// writer gate is acquired. Tests arm a late recovery intent in this window
    /// and prove the under-branch-gate check refuses to advance around it.
    pub const OPTIMIZE_POST_RECOVERY_CHECK_PRE_MAIN_GATE: &str =
        "optimize.post_recovery_check_pre_main_gate";
    pub const OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT: &str =
        "optimize.post_phase_b_pre_manifest_commit";
    pub const RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH: &str = "recovery.before_roll_forward_publish";
    /// Recovery has listed/parsed its discovery snapshot but has not yet taken
    /// per-sidecar gates. Tests rewrite confirmation state in this window.
    pub const RECOVERY_POST_LIST_PRE_GATES: &str = "recovery.post_list_pre_gates";
    pub const RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND: &str = "recovery.orphan_discard_audit_append";
    /// After the fixed rollback lineage/table-pin publish is durable, before
    /// its operator-facing audit row is appended.
    pub const RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT: &str =
        "recovery.post_rollback_publish_pre_audit";
    /// After recovery restores one table to its prepared pre-effect content,
    /// before the compensating manifest publish. A retry must recognize that
    /// restore as this sidecar's owned compensation instead of wedging open.
    pub const RECOVERY_POST_TABLE_RESTORE_PRE_PUBLISH: &str =
        "recovery.post_table_restore_pre_publish";
    pub const RECOVERY_RECORD_AUDIT: &str = "recovery.record_audit";
    pub const RECOVERY_SIDECAR_CONFIRM: &str = "recovery.sidecar_confirm";
    pub const RECOVERY_SIDECAR_DELETE: &str = "recovery.sidecar_delete";
    pub const RECOVERY_SIDECAR_LIST: &str = "recovery.sidecar_list";
    /// After recovery discovery lists and sorts `__recovery/`, before it reads
    /// the first sidecar body. Tests let a live writer publish and delete its
    /// sidecar in this window, proving a raced NotFound is concurrent
    /// completion rather than a storage failure.
    pub const RECOVERY_POST_SIDECAR_LIST_PRE_READ: &str =
        "recovery.post_sidecar_list_pre_read";
    pub const RECOVERY_SIDECAR_WRITE: &str = "recovery.sidecar_write";
    pub const SCHEMA_APPLY_AFTER_MANIFEST_COMMIT: &str = "schema_apply.after_manifest_commit";
    pub const SCHEMA_APPLY_AFTER_STAGING_WRITE: &str = "schema_apply.after_staging_write";
    pub const SCHEMA_APPLY_BEFORE_STAGING_WRITE: &str = "schema_apply.before_staging_write";
    /// The schema-v7 ownership sidecar is durable, but no table transaction
    /// has been staged or committed yet. Tests use this to install a genuinely
    /// foreign first-touch dataset winner.
    pub const SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT: &str = "schema_apply.post_sidecar_pre_effect";
    /// After each exact SchemaApply table transaction commits, before the next
    /// table effect or durable EffectsConfirmed transition.
    pub const SCHEMA_APPLY_POST_TABLE_COMMIT: &str = "schema_apply.post_table_commit";
    /// RFC-026 enrollment intent is durable, before the singleton MemWAL index
    /// initializer can advance the base table from N to N+1.
    pub const STREAM_ENROLLMENT_POST_SIDECAR_PRE_INDEX: &str =
        "stream_enrollment.post_sidecar_pre_index";
    /// The initializer returned after its internal commit, before the shared
    /// exact recovery adapter classifies/provisions/publishes the enrollment.
    pub const STREAM_ENROLLMENT_POST_INDEX_PRE_RECOVERY: &str =
        "stream_enrollment.post_index_pre_recovery";
    /// The pre-minted shard is durably present at exact empty epoch 1, before
    /// the table pointer and OPEN lifecycle land in one manifest CAS.
    pub const STREAM_ENROLLMENT_POST_SHARD_PRE_MANIFEST: &str =
        "stream_enrollment.post_shard_pre_manifest";
    /// B1 completed every row-effect-free check and reservation but has not
    /// invoked `ShardWriter::put_no_wait` yet.
    pub const STREAM_B1_BEFORE_PUT_INVOKE: &str = "stream_b1.before_put_invoke";
    /// A charged same-key waiter owns shared fold admission immediately before
    /// it waits for the current input corridor owner to transfer its charge.
    pub const STREAM_B1_AFTER_SHARED_BEFORE_QUEUE_WAIT: &str =
        "stream_b1.after_shared_before_queue_wait";
    /// A put owns its exact Arrow charge, shared admission, and the same-key
    /// input queue, but has not begun warm validation or a cold writer claim.
    /// Tests park a cold replay opener here so already-charged waiters exist
    /// when replay accounting is installed.
    pub const STREAM_B1_AFTER_INPUT_QUEUE_BEFORE_PREPARE: &str =
        "stream_b1.after_input_queue_before_prepare";
    /// A cold put classified the claimed writer, including exact replay
    /// accounting, before its final graph-authority recheck. Failure here must
    /// retain the opened disposition through owned retirement.
    pub const STREAM_B1_AFTER_COLD_CLASSIFY_BEFORE_FINAL_AUTHORITY: &str =
        "stream_b1.after_cold_classify_before_final_authority";
    /// `put_no_wait` was invoked, so every later outcome is acknowledgement-
    /// ambiguous even if the durability watcher has not yet been obtained.
    pub const STREAM_B1_AFTER_PUT_INVOKE_BEFORE_WATCHER: &str =
        "stream_b1.after_put_invoke_before_watcher";
    pub const STREAM_B1_BEFORE_WATCHER_WAIT: &str = "stream_b1.before_watcher_wait";
    pub const STREAM_B1_AFTER_WATCHER_SUCCESS: &str = "stream_b1.after_watcher_success";
    /// The fold worker owns exclusive admission and is about to replace the
    /// one active MemTable with a frozen generation.
    pub const STREAM_B1_BEFORE_FORCE_SEAL: &str = "stream_b1.before_force_seal";
    pub const STREAM_B1_AFTER_FORCE_SEAL: &str = "stream_b1.after_force_seal";
    /// Lance reported the frozen queue drained, before the independent shard-
    /// manifest and in-memory-ref proof required by RFC-026.
    pub const STREAM_B1_AFTER_FLUSH_DRAIN_BEFORE_PROOF: &str =
        "stream_b1.after_flush_drain_before_proof";
    pub const STREAM_B1_AFTER_POST_DRAIN_PROOF: &str = "stream_b1.after_post_drain_proof";
    pub const STREAM_B1_BEFORE_ABORT: &str = "stream_b1.before_abort";
    /// Test-only rendezvous proving the original abort future remains owned by
    /// the retired registry entry and is never replaced by a second abort.
    pub const STREAM_B1_ABORT_STALL: &str = "stream_b1.abort_stall";
    /// A retained abort settled, the registry slot/counters were released,
    /// and the admission lease was dropped. This is a test-only quiescence
    /// witness for deterministic replay-open races.
    pub const STREAM_B1_AFTER_RETIREMENT_RELEASE: &str = "stream_b1.after_retirement_release";
    /// The immutable generation cut is proven and the worker retired, before
    /// the schema-v11 recovery intent is armed.
    pub const STREAM_FOLD_POST_DRAIN_PRE_SIDECAR: &str = "stream_fold.post_drain_pre_sidecar";
    /// The exact Lance Update (rows plus merged-generation marker) committed,
    /// before its achieved identity and output are durably confirmed.
    pub const STREAM_FOLD_POST_TABLE_COMMIT_PRE_CONFIRM: &str =
        "stream_fold.post_table_commit_pre_confirm";
    /// Reload owns the schema gate and is about to read/publish one contract view.
    pub const SCHEMA_RELOAD_BEFORE_CONTRACT_READ: &str = "schema_reload.before_contract_read";
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
