//! The compile-checked catalog of every decision seam in this crate: one
//! static per site, named by the string a case file uses. `ALL` lists them
//! for the runner and the guard.

use omnigraph_seams::{DecideSeam, Effect, Global, Op, Seam, SeamEntry};

pub static UPGRADE_AFTER_FENCE: DecideSeam = Seam::decide(
    "upgrade.after_fence",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static UPGRADE_AFTER_STAGE: DecideSeam = Seam::decide(
    "upgrade.after_stage",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static UPGRADE_AFTER_BRANCH: DecideSeam = Seam::decide(
    "upgrade.after_branch",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static UPGRADE_BEFORE_ACTIVATION: DecideSeam = Seam::decide(
    "upgrade.before_activation",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static UPGRADE_AFTER_ACTIVATION: DecideSeam = Seam::decide(
    "upgrade.after_activation",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After Lance returns success from its two-phase native create, before
/// OmniGraph acknowledges it. Recovery must classify the matching
/// BranchContents as a completed create (lost acknowledgement).
pub static BRANCH_CREATE_POST_NATIVE: DecideSeam = Seam::decide(
    "branch_create.post_native",
    Op::BranchCreate,
    Effect::Fail,
    Global::new(),
);
/// After Lance returns success from native delete, before OmniGraph
/// acknowledges it. Recovery must classify the absent BranchContents as a
/// completed logical deletion.
pub static BRANCH_DELETE_POST_NATIVE: DecideSeam = Seam::decide(
    "branch_delete.post_native",
    Op::BranchDelete,
    Effect::Fail,
    Global::new(),
);
/// Branch delete holds the schema, target-branch, and fresh-catalog table
/// envelope and has completed its final recovery check, before the native
/// manifest-ref mutation.
pub static BRANCH_DELETE_POST_TABLE_GATES: DecideSeam = Seam::decide(
    "branch_delete.post_table_gates",
    Op::BranchDelete,
    Effect::Fail,
    Global::new(),
);
/// After native branch control completed its first recovery barrier, before
/// it acquires schema -> branch -> table gates and performs the final check.
pub static BRANCH_CONTROL_POST_RECOVERY_BARRIER: DecideSeam = Seam::decide(
    "branch_control.post_recovery_barrier",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT: DecideSeam = Seam::decide(
    "branch_merge.adopt_after_append_pre_upsert",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
/// After one bounded strict-insert chunk committed while at least one later
/// chunk from the same Armed BranchMerge transaction chain remains.
pub static BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS: DecideSeam = Seam::decide(
    "branch_merge.adopt_between_insert_chunks",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE: DecideSeam = Seam::decide(
    "branch_merge.adopt_after_upsert_pre_delete",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
/// After one bounded delete chunk committed while at least one later
/// delete chunk from the same Armed BranchMerge chain remains.
pub static BRANCH_MERGE_BETWEEN_DELETE_CHUNKS: DecideSeam = Seam::decide(
    "branch_merge.between_delete_chunks",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
/// Source/target heads and snapshots have been captured while the schema
/// and both branch-incarnation gates are held, before merge planning or
/// any durable table effect.
pub static BRANCH_MERGE_POST_AUTHORITY_CAPTURE: DecideSeam = Seam::decide(
    "branch_merge.post_authority_capture",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
/// Candidate classification and validation have completed, before the
/// final source/target table-gate envelope and recovery arm. Tests use this
/// boundary to prove a raw source-table ref delete/recreate cannot pass as
/// the native incarnation whose immutable rows were proven.
pub static BRANCH_MERGE_POST_CANDIDATE_VALIDATION: DecideSeam = Seam::decide(
    "branch_merge.post_candidate_validation",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
/// The v4 BranchMerge recovery intent is durable, before any first-touch
/// target table ref is created.
pub static BRANCH_MERGE_POST_SIDECAR_PRE_FORK: DecideSeam = Seam::decide(
    "branch_merge.post_sidecar_pre_fork",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT: DecideSeam = Seam::decide(
    "branch_merge.post_phase_b_pre_manifest_commit",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
/// Every merge table effect is complete, but the sidecar is still in its
/// pre-confirmation shape.
pub static BRANCH_MERGE_POST_EFFECTS_PRE_CONFIRM: DecideSeam = Seam::decide(
    "branch_merge.post_effects_pre_confirm",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_PRE_ERROR_RECOVERY: DecideSeam = Seam::decide(
    "branch_merge.pre_error_recovery",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM: DecideSeam = Seam::decide(
    "branch_merge.rewrite_after_delete_pre_confirm",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE: DecideSeam = Seam::decide(
    "branch_merge.rewrite_after_merge_pre_delete",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static BRANCH_MERGE_REWRITE_AFTER_INSERT_PRE_UPDATE: DecideSeam = Seam::decide(
    "branch_merge.rewrite_after_insert_pre_update",
    Op::BranchMerge,
    Effect::Fail,
    Global::new(),
);
pub static CLASSIFY_FRESH_READ: DecideSeam = Seam::decide(
    "classify.fresh_read",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// A Blob read has captured one exact graph snapshot and table authority,
/// but has not opened the selected Lance table version yet. Tests replace
/// a named branch here to prove a live read fails rather than retargeting.
pub static BLOB_READ_POST_CAPTURE: DecideSeam = Seam::decide(
    "blob_read.post_capture",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// A change-feed poll has captured its cut, but has not reopened any
/// commit's per-branch manifest snapshot yet. Tests delete and recreate a
/// named branch here to prove the poll fails closed rather than emitting the
/// replacement branch's rows under the captured commit's label.
pub static CHANGE_FEED_POST_CAPTURE: DecideSeam = Seam::decide(
    "change_feed.post_capture",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// A change-feed poll has reopened and re-proven a commit's manifest
/// snapshot, but has not yet opened the per-table datasets it names. Tests
/// delete and recreate a named branch here to prove the physical table open
/// re-proves the branch incarnation (via the manifest e_tag) rather than
/// reading the replacement branch's rows at the same path and version.
pub static CHANGE_FEED_PRE_TABLE_OPEN: DecideSeam = Seam::decide(
    "change_feed.pre_table_open",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// A skip seam, not an error injection: simulates a store
/// whose persisted table version metadata carries no e_tag, so
/// `open_at_entry_verified` cannot use its e_tag comparison. Tests combine
/// it with `CHANGE_FEED_PRE_TABLE_OPEN` + a branch delete/recreate to prove
/// the LOGICAL post-open head re-prove still refuses the replacement —
/// the e_tag is defense-in-depth, not the load-bearing witness.
pub static CHANGE_FEED_SKIP_ETAG_WITNESS: DecideSeam = Seam::decide(
    "change_feed.skip_etag_witness",
    Op::Unreachable,
    Effect::Skip,
    Global::new(),
);
/// A change-feed poll has passed the final post-open logical head witness
/// for one commit, and is about to plan each interval's emitter. Tests
/// delete and recreate a named branch here: any live read of the branch's
/// numeric-path history after this point (the replaceable read — version
/// manifests sit at numeric paths, unlike UUID-named data and transaction
/// files) would classify the interval from the REPLACEMENT branch's
/// transactions and can silently omit the original commit's deletes.
pub static CHANGE_FEED_POST_HEAD_WITNESS: DecideSeam = Seam::decide(
    "change_feed.post_head_witness",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static CLEANUP_RECONCILE_FORK: DecideSeam = Seam::decide(
    "cleanup.reconcile_fork",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After cleanup's fast empty-sidecar probe, before it acquires the closed
/// schema/branch/table GC gate set and performs the authoritative recheck.
pub static CLEANUP_POST_RECOVERY_CHECK_PRE_GATES: DecideSeam = Seam::decide(
    "cleanup.post_recovery_check_pre_gates",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static CLEANUP_RESOLVE_BRANCH_SNAPSHOT: DecideSeam = Seam::decide(
    "cleanup.resolve_branch_snapshot",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static CLEANUP_TABLE_GC: DecideSeam = Seam::decide(
    "cleanup.table_gc",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT: DecideSeam = Seam::decide(
    "ensure_indices.post_phase_b_pre_manifest_commit",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Every exact index transaction and first-touch ref effect is durable,
/// but the v8 sidecar is still Armed. Recovery must therefore compensate
/// rather than infer the intended manifest delta from physical state.
pub static ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM: DecideSeam = Seam::decide(
    "ensure_indices.post_effects_pre_confirm",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static ENSURE_INDICES_POST_SIDECAR_PRE_FORK: DecideSeam = Seam::decide(
    "ensure_indices.post_sidecar_pre_fork",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static ENSURE_INDICES_POST_TABLE_EFFECT: DecideSeam = Seam::decide(
    "ensure_indices.post_table_effect",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE: DecideSeam = Seam::decide(
    "ensure_indices.post_stage_pre_commit_btree",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static FORK_BEFORE_CLASSIFY: DecideSeam = Seam::decide(
    "fork.before_classify",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// After Lance durably creates a target table ref, before the caller can
/// reopen and verify it. An error here is post-effect and must retain the
/// recovery sidecar.
pub static FORK_POST_CREATE_PRE_OPEN: DecideSeam = Seam::decide(
    "fork.post_create_pre_open",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT: DecideSeam = Seam::decide(
    "graph_publish.after_manifest_commit",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static GRAPH_PUBLISH_BEFORE_COMMIT_APPEND: DecideSeam = Seam::decide(
    "graph_publish.before_commit_append",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// Fires past init's commit point — the graph must survive errors
/// injected here.
pub static INIT_AFTER_COORDINATOR_INIT: DecideSeam = Seam::decide(
    "init.after_coordinator_init",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static INIT_AFTER_SCHEMA_CONTRACT_WRITTEN: DecideSeam = Seam::decide(
    "init.after_schema_contract_written",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static INIT_AFTER_SCHEMA_PG_WRITTEN: DecideSeam = Seam::decide(
    "init.after_schema_pg_written",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After Lance has durably returned the new `__manifest` Dataset, but
/// before OmniGraph's create half can acknowledge it. Returning an error
/// here models a lost object-store acknowledgement and must route through
/// exact-genesis classification rather than schema cleanup.
pub static INIT_MANIFEST_CREATE_ACK_LOST: DecideSeam = Seam::decide(
    "init.manifest_create_ack_lost",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Before the durable exact-genesis probe used to classify an
/// acknowledgement-unknown manifest Create. An injected failure proves the
/// caller preserves schema artifacts when the outcome cannot be observed.
pub static INIT_MANIFEST_CREATE_PROBE: DecideSeam = Seam::decide(
    "init.manifest_create_probe",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After a per-type Lance dataset Create returns success, before graph
/// initialization can acknowledge it. The graph manifest does not exist
/// yet, but retry and schema cleanup are unsafe because the table Create
/// may be durable.
pub static INIT_TABLE_CREATE_ACK_LOST: DecideSeam = Seam::decide(
    "init.table_create_ack_lost",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Inject an indeterminate schema-artifact delete during pre-physical init
/// cleanup. The original init error must win and the durable claim must be
/// retained so a delayed delete cannot race another initializer.
pub static INIT_SCHEMA_CLEANUP_DELETE: DecideSeam = Seam::decide(
    "init.schema_cleanup_delete",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// The first ordinary post-commit read-back failpoint after the graph's
/// `__manifest` Create has been positively classified. A crash OR an error
/// return here must leave an openable graph; init's schema cleanup is
/// unreachable from this window (issue #495).
pub static INIT_POST_MANIFEST_CREATE: DecideSeam = Seam::decide(
    "init.post_manifest_create",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// A read-write bind of a local graph root, before the create-if-absent
/// probe writes its probe object. Injecting here simulates a filesystem
/// without hard-link support (issue #453) for both `init` and
/// read-write `open`.
pub static LOCAL_CREATE_IF_ABSENT_PROBE: DecideSeam = Seam::decide(
    "storage.local_create_if_absent_probe",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// The implicit fork-if-missing branch
/// create completed durably, before any load staging byte is written.
/// The load "never happened" yet its target branch exists — a failed
/// load's surviving empty branch.
pub static LOAD_POST_BRANCH_CREATE_PRE_STAGE: DecideSeam = Seam::decide(
    "load.post_branch_create_pre_stage",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Between per-table fragment uploads
/// inside `stage_all_with_concurrency`. A crash here
/// leaves a PARTIAL set of staged files across tables with no breadcrumb
/// — benign by construction (unreferenced, reclaimable by cleanup), which
/// is exactly what the window lets a universe prove.
pub static LOAD_BETWEEN_TABLE_STAGES: DecideSeam = Seam::decide(
    "load.between_table_stages",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE: DecideSeam = Seam::decide(
    "mutation.delete_node_pre_primary_delete",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// After every deferred first-touch table ref is created under a durable
/// v3 sidecar, before any staged data transaction advances target HEAD.
pub static MUTATION_POST_FORK_PRE_COMMIT: DecideSeam = Seam::decide(
    "mutation.post_fork_pre_commit",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// After each exact staged table transaction advances HEAD, before the next
/// table effect or Phase-B confirmation. Used to leave a real partial
/// multi-table v3 attempt whose remaining first-touch fork still needs
/// recovery cleanup.
pub static MUTATION_POST_TABLE_COMMIT: DecideSeam = Seam::decide(
    "mutation.post_table_commit",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// After the v3 ownership sidecar is durable but before the first deferred
/// named-table ref is created. Recovery must accept the absent target ref.
pub static MUTATION_POST_SIDECAR_PRE_FORK: DecideSeam = Seam::decide(
    "mutation.post_sidecar_pre_fork",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// The v9 mutation/load recovery intent is durable (Armed) but no table
/// transaction has committed yet — the window where a writer failure or
/// cancellation strands an effect-free sidecar (issue #554). Unlike
/// `MUTATION_POST_SIDECAR_PRE_FORK`, this fires for every enrolled
/// mutation/load, main-branch writes included.
pub static MUTATION_POST_ARM_PRE_EFFECT: DecideSeam = Seam::decide(
    "mutation.post_arm_pre_effect",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// Deterministic OCC rendezvous after a mutation has validated and staged
/// its complete attempt, but before the RFC-022 branch effect gate is
/// acquired and the write authority token is revalidated. Tests park the
/// first writer here, commit a conflicting second writer, then prove the
/// first attempt is discarded and validation is rerun from a fresh token.
pub static MUTATION_POST_STAGE_PRE_EFFECT_GATE: DecideSeam = Seam::decide(
    "mutation.post_stage_pre_effect_gate",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// After a conditional mutation has executed to a zero-effect result, but
/// before it acquires the branch gate and revalidates the caller's graph
/// head. This pins the linearization point for successful no-op CAS calls.
pub static MUTATION_POST_NO_EFFECT_PRE_GATE: DecideSeam = Seam::decide(
    "mutation.post_no_effect_pre_gate",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
pub static MUTATION_POST_FINALIZE_PRE_PUBLISHER: DecideSeam = Seam::decide(
    "mutation.post_finalize_pre_publisher",
    Op::Mutation,
    Effect::Fail,
    Global::new(),
);
/// A stale live read has opened and decoded a replacement manifest whose
/// exact branch-head row is absent, but has not yet decoded the inherited
/// lineage fallback. Failure here must leave the old coordinator coherent.
/// Crossed by reads only, which no case step kind names yet.
pub static READ_REFRESH_POST_STATE_PRE_LINEAGE: DecideSeam = Seam::decide(
    "read.refresh_post_state_pre_lineage",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Open owns the schema gate and is about to read source/IR/state as one
/// catalog view.
pub static OPEN_BEFORE_SCHEMA_CONTRACT_READ: DecideSeam = Seam::decide(
    "open.before_schema_contract_read",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static OPTIMIZE_BEFORE_COMPACT: DecideSeam = Seam::decide(
    "optimize.before_compact",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static OPTIMIZE_INJECT_REINDEX_CONFLICT: DecideSeam = Seam::decide(
    "optimize.inject_reindex_conflict",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After Optimize captures its authority token, before the schema -> main
/// -> table gates and the revalidation that consumes it. Tests advance the
/// graph in this window and prove Optimize refuses rather than planning
/// against authority that has already moved.
pub static OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES: DecideSeam = Seam::decide(
    "optimize.post_authority_capture_pre_gates",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After Optimize's broad recovery fast-path check, before the main-branch
/// writer gate is acquired. Tests arm a late recovery intent in this window
/// and prove the under-branch-gate check refuses to advance around it.
pub static OPTIMIZE_POST_RECOVERY_CHECK_PRE_MAIN_GATE: DecideSeam = Seam::decide(
    "optimize.post_recovery_check_pre_main_gate",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT: DecideSeam = Seam::decide(
    "optimize.post_phase_b_pre_manifest_commit",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH: DecideSeam = Seam::decide(
    "recovery.before_roll_forward_publish",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// Recovery has listed/parsed its discovery snapshot but has not yet taken
/// per-sidecar gates. Tests rewrite confirmation state in this window.
pub static RECOVERY_POST_LIST_PRE_GATES: DecideSeam = Seam::decide(
    "recovery.post_list_pre_gates",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND: DecideSeam = Seam::decide(
    "recovery.orphan_discard_audit_append",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// After the fixed rollback lineage/table-pin publish is durable, before
/// its operator-facing audit row is appended.
pub static RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT: DecideSeam = Seam::decide(
    "recovery.post_rollback_publish_pre_audit",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// After recovery restores one table to its prepared pre-effect content,
/// before the compensating manifest publish. A retry must recognize that
/// restore as this sidecar's owned compensation instead of wedging open.
pub static RECOVERY_POST_TABLE_RESTORE_PRE_PUBLISH: DecideSeam = Seam::decide(
    "recovery.post_table_restore_pre_publish",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_RECORD_AUDIT: DecideSeam = Seam::decide(
    "recovery.record_audit",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_SIDECAR_CONFIRM: DecideSeam = Seam::decide(
    "recovery.sidecar_confirm",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_SIDECAR_DELETE: DecideSeam = Seam::decide(
    "recovery.sidecar_delete",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_SIDECAR_LIST: DecideSeam = Seam::decide(
    "recovery.sidecar_list",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
/// After recovery discovery lists and sorts `__recovery/`, before it reads
/// the first sidecar body. Tests let a live writer publish and delete its
/// sidecar in this window, proving a raced NotFound is concurrent
/// completion rather than a storage failure.
pub static RECOVERY_POST_SIDECAR_LIST_PRE_READ: DecideSeam = Seam::decide(
    "recovery.post_sidecar_list_pre_read",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static RECOVERY_SIDECAR_WRITE: DecideSeam = Seam::decide(
    "recovery.sidecar_write",
    Op::AnyWrite,
    Effect::Fail,
    Global::new(),
);
pub static SCHEMA_APPLY_AFTER_MANIFEST_COMMIT: DecideSeam = Seam::decide(
    "schema_apply.after_manifest_commit",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static SCHEMA_APPLY_AFTER_STAGING_WRITE: DecideSeam = Seam::decide(
    "schema_apply.after_staging_write",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
pub static SCHEMA_APPLY_BEFORE_STAGING_WRITE: DecideSeam = Seam::decide(
    "schema_apply.before_staging_write",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// The schema-v7 ownership sidecar is durable, but no table transaction
/// has been staged or committed yet. Tests use this to install a genuinely
/// foreign first-touch dataset winner.
pub static SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT: DecideSeam = Seam::decide(
    "schema_apply.post_sidecar_pre_effect",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// After each exact SchemaApply table transaction commits, before the next
/// table effect or durable EffectsConfirmed transition.
pub static SCHEMA_APPLY_POST_TABLE_COMMIT: DecideSeam = Seam::decide(
    "schema_apply.post_table_commit",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// The RFC 0040 system-column upgrade advanced main's `__manifest` stamp
/// but has renamed no table yet: stamp 9 over legacy spellings under an
/// Armed intent, the one state no other writer can produce.
pub static SYSTEM_COLUMN_UPGRADE_AFTER_STAMP_ADVANCE: DecideSeam = Seam::decide(
    "system_column_upgrade.after_stamp_advance",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Recovery of a system-column upgrade reclaimed the crashed writer's
/// `__schema_apply_lock__` but has not retired the intent yet: the sidecar
/// alone re-enters cleanup, the lock is already gone.
pub static SYSTEM_COLUMN_UPGRADE_AFTER_LOCK_RECLAIM: DecideSeam = Seam::decide(
    "system_column_upgrade.after_lock_reclaim",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Reload owns the schema gate and is about to read/publish one contract view.
pub static SCHEMA_RELOAD_BEFORE_CONTRACT_READ: DecideSeam = Seam::decide(
    "schema_reload.before_contract_read",
    Op::Unreachable,
    Effect::Fail,
    Global::new(),
);
/// Injects a retryable `RowLevelCasContention` from `load_publish_state` so a
/// test can prove the publisher's outer retry re-runs the load.
pub static PUBLISH_LOAD_STATE_RETRYABLE_CONTENTION: DecideSeam = Seam::decide(
    "publish.load_state_retryable_contention",
    Op::AnyWrite,
    Effect::Contention,
    Global::new(),
);

/// The put that moves a mutation's sidecar from `Armed` to `EffectsConfirmed`,
/// after every confirm-time check. Skipped: the engine believes it confirmed
/// and publishes, the object keeps its arm-time bytes (a lost write). Alone
/// the post-publish delete removes the stale object; skipped together with
/// `MUTATION_SIDECAR_POST_PUBLISH_DELETE` it leaves the Armed-beside-visible-
/// commit shape of issue #602.
pub static MUTATION_SIDECAR_CONFIRM_PUT: DecideSeam = Seam::decide(
    "mutation.sidecar_confirm_put",
    Op::Mutation,
    Effect::Skip,
    Global::new(),
);
/// The delete of a mutation's sidecar once its `__manifest` commit is visible.
/// Skipped: the object survives the publish with whatever bytes it holds (a
/// lost write), the residue the next read-write open finalizes.
pub static MUTATION_SIDECAR_POST_PUBLISH_DELETE: DecideSeam = Seam::decide(
    "mutation.sidecar_post_publish_delete",
    Op::Mutation,
    Effect::Skip,
    Global::new(),
);

/// Every decision seam in this crate.
pub static ALL: &[&'static dyn SeamEntry] = &[
    &MUTATION_SIDECAR_CONFIRM_PUT,
    &MUTATION_SIDECAR_POST_PUBLISH_DELETE,
    &UPGRADE_AFTER_FENCE,
    &UPGRADE_AFTER_STAGE,
    &UPGRADE_AFTER_BRANCH,
    &UPGRADE_BEFORE_ACTIVATION,
    &UPGRADE_AFTER_ACTIVATION,
    &BRANCH_CREATE_POST_NATIVE,
    &BRANCH_DELETE_POST_NATIVE,
    &BRANCH_DELETE_POST_TABLE_GATES,
    &BRANCH_CONTROL_POST_RECOVERY_BARRIER,
    &BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT,
    &BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS,
    &BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE,
    &BRANCH_MERGE_BETWEEN_DELETE_CHUNKS,
    &BRANCH_MERGE_POST_AUTHORITY_CAPTURE,
    &BRANCH_MERGE_POST_CANDIDATE_VALIDATION,
    &BRANCH_MERGE_POST_SIDECAR_PRE_FORK,
    &BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    &BRANCH_MERGE_POST_EFFECTS_PRE_CONFIRM,
    &BRANCH_MERGE_PRE_ERROR_RECOVERY,
    &BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM,
    &BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE,
    &BRANCH_MERGE_REWRITE_AFTER_INSERT_PRE_UPDATE,
    &CLASSIFY_FRESH_READ,
    &BLOB_READ_POST_CAPTURE,
    &CHANGE_FEED_POST_CAPTURE,
    &CHANGE_FEED_PRE_TABLE_OPEN,
    &CHANGE_FEED_SKIP_ETAG_WITNESS,
    &CHANGE_FEED_POST_HEAD_WITNESS,
    &CLEANUP_RECONCILE_FORK,
    &CLEANUP_POST_RECOVERY_CHECK_PRE_GATES,
    &CLEANUP_RESOLVE_BRANCH_SNAPSHOT,
    &CLEANUP_TABLE_GC,
    &ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    &ENSURE_INDICES_POST_EFFECTS_PRE_CONFIRM,
    &ENSURE_INDICES_POST_SIDECAR_PRE_FORK,
    &ENSURE_INDICES_POST_TABLE_EFFECT,
    &ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
    &FORK_BEFORE_CLASSIFY,
    &FORK_POST_CREATE_PRE_OPEN,
    &GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT,
    &GRAPH_PUBLISH_BEFORE_COMMIT_APPEND,
    &INIT_AFTER_COORDINATOR_INIT,
    &INIT_AFTER_SCHEMA_CONTRACT_WRITTEN,
    &INIT_AFTER_SCHEMA_PG_WRITTEN,
    &INIT_MANIFEST_CREATE_ACK_LOST,
    &INIT_MANIFEST_CREATE_PROBE,
    &INIT_TABLE_CREATE_ACK_LOST,
    &INIT_SCHEMA_CLEANUP_DELETE,
    &INIT_POST_MANIFEST_CREATE,
    &LOCAL_CREATE_IF_ABSENT_PROBE,
    &LOAD_POST_BRANCH_CREATE_PRE_STAGE,
    &LOAD_BETWEEN_TABLE_STAGES,
    &MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE,
    &MUTATION_POST_FORK_PRE_COMMIT,
    &MUTATION_POST_TABLE_COMMIT,
    &MUTATION_POST_SIDECAR_PRE_FORK,
    &MUTATION_POST_ARM_PRE_EFFECT,
    &MUTATION_POST_STAGE_PRE_EFFECT_GATE,
    &MUTATION_POST_NO_EFFECT_PRE_GATE,
    &MUTATION_POST_FINALIZE_PRE_PUBLISHER,
    &READ_REFRESH_POST_STATE_PRE_LINEAGE,
    &OPEN_BEFORE_SCHEMA_CONTRACT_READ,
    &OPTIMIZE_BEFORE_COMPACT,
    &OPTIMIZE_INJECT_REINDEX_CONFLICT,
    &OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES,
    &OPTIMIZE_POST_RECOVERY_CHECK_PRE_MAIN_GATE,
    &OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    &RECOVERY_BEFORE_ROLL_FORWARD_PUBLISH,
    &RECOVERY_POST_LIST_PRE_GATES,
    &RECOVERY_ORPHAN_DISCARD_AUDIT_APPEND,
    &RECOVERY_POST_ROLLBACK_PUBLISH_PRE_AUDIT,
    &RECOVERY_POST_TABLE_RESTORE_PRE_PUBLISH,
    &RECOVERY_RECORD_AUDIT,
    &RECOVERY_SIDECAR_CONFIRM,
    &RECOVERY_SIDECAR_DELETE,
    &RECOVERY_SIDECAR_LIST,
    &RECOVERY_POST_SIDECAR_LIST_PRE_READ,
    &RECOVERY_SIDECAR_WRITE,
    &SCHEMA_APPLY_AFTER_MANIFEST_COMMIT,
    &SCHEMA_APPLY_AFTER_STAGING_WRITE,
    &SCHEMA_APPLY_BEFORE_STAGING_WRITE,
    &SCHEMA_APPLY_POST_SIDECAR_PRE_EFFECT,
    &SCHEMA_APPLY_POST_TABLE_COMMIT,
    &SYSTEM_COLUMN_UPGRADE_AFTER_STAMP_ADVANCE,
    &SYSTEM_COLUMN_UPGRADE_AFTER_LOCK_RECLAIM,
    &SCHEMA_RELOAD_BEFORE_CONTRACT_READ,
    &PUBLISH_LOAD_STATE_RETRYABLE_CONTENTION,
];

/// The one string-keyed lookup: a case file or the harness names a seam, the catalog answers.
pub fn decide(name: &str) -> Option<&'static DecideSeam> {
    ALL.iter()
        .find(|entry| entry.name() == name)
        .and_then(|entry| entry.as_decide())
}

/// Empty every decision seam (a scenario teardown).
#[cfg(feature = "failpoints")]
pub fn clear_all() {
    for entry in ALL {
        entry.clear();
    }
}
