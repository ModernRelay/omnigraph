use crate::error::Result;
#[cfg(feature = "failpoints")]
use std::sync::Arc;

#[cfg(feature = "failpoints")]
use fail_parallel::FailPointRegistry;

// The failpoint registry is a VALUE (`fail-parallel`) held behind one
// swappable crate-global handle: `registry()` exposes it, `set_registry()`
// swaps it. Reifying the registry is what lets a test harness own its
// faults per-universe; until callers thread a registry of their own,
// everything shares this default and scenario-holding tests serialize on
// `SCENARIO_GATE` below.
#[cfg(feature = "failpoints")]
mod fp_registry {
    use std::sync::{Arc, LazyLock, RwLock};

    use fail_parallel::FailPointRegistry;

    static CURRENT: LazyLock<RwLock<Arc<FailPointRegistry>>> =
        LazyLock::new(|| RwLock::new(Arc::new(FailPointRegistry::new())));

    /// The registry consulted by every failpoint crossing.
    pub fn registry() -> Arc<FailPointRegistry> {
        CURRENT
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Swap in a caller-owned registry; returns the previous one.
    /// No in-tree caller yet: kept (documented interim, #527 P2) for the
    /// planned per-universe registry isolation; until then scenario tests
    /// serialize on `SCENARIO_GATE` and every guard pins its registry.
    pub fn set_registry(next: Arc<FailPointRegistry>) -> Arc<FailPointRegistry> {
        std::mem::replace(
            &mut *CURRENT
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            next,
        )
    }
}
#[cfg(feature = "failpoints")]
pub use fp_registry::{registry, set_registry};

/// Serializes scenario-holding tests: everything shares the one default
/// registry, so concurrent scenarios would clear and fire each other's
/// points. Kept call-shape-compatible with the `fail` crate this
/// replaced, whose global scenario mutex provided the same serialization.
#[cfg(feature = "failpoints")]
static SCENARIO_GATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// RAII failpoint scenario over the crate registry. Holds
/// [`SCENARIO_GATE`] for its lifetime so scenario-holding tests
/// serialize; a poisoned gate is recovered, so one panicking test cannot
/// wedge the rest of the suite.
#[cfg(feature = "failpoints")]
pub struct FailScenario {
    // Field order: `_inner` must drop first (clearing the registry while
    // the gate is still held), then `_gate` releases.
    _inner: fail_parallel::FailScenario,
    _gate: std::sync::MutexGuard<'static, ()>,
}

#[cfg(feature = "failpoints")]
impl FailScenario {
    /// Acquires the scenario gate — blocks until no other scenario is
    /// live — then opens a fresh scenario on the registry.
    pub fn setup() -> Self {
        let gate = SCENARIO_GATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Self {
            _inner: fail_parallel::FailScenario::setup(registry()),
            _gate: gate,
        }
    }

    /// Clears the registry and releases the gate.
    pub fn teardown(self) {
        self._inner.teardown();
    }
}

pub(crate) fn maybe_fail(_name: &str) -> Result<()> {
    #[cfg(feature = "failpoints")]
    {
        let name = _name;
        fail_parallel::fail_point!(registry(), name, |_| {
            Err(crate::error::OmniError::manifest(format!(
                "injected failpoint triggered: {}",
                name
            )))
        });
    }
    Ok(())
}

/// Boolean behavior seam: true when the named failpoint is configured (any
/// action). Unlike [`maybe_fail`], it injects no error — a test uses it to
/// flip a code path into a configuration that cannot be reached naturally on
/// the local test substrate (e.g. simulating an object store that persists no
/// table e_tags). Always false without the `failpoints` feature.
pub(crate) fn is_enabled(_name: &str) -> bool {
    #[cfg(feature = "failpoints")]
    fail_parallel::fail_point!(registry(), _name, |_| true);
    false
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
        fail_parallel::fail_point!(registry(), name, |_| {
            Err(crate::error::OmniError::manifest_row_level_cas_contention(
                format!("injected retryable contention failpoint: {name}"),
            ))
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
    pub const BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM: &str =
        "branch_merge.rewrite_after_delete_pre_confirm";
    pub const BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE: &str =
        "branch_merge.rewrite_after_merge_pre_delete";
    pub const CLASSIFY_FRESH_READ: &str = "classify.fresh_read";
    /// A Blob read has captured one exact graph snapshot and table authority,
    /// but has not opened the selected Lance table version yet. Tests replace
    /// a named branch here to prove a live read fails rather than retargeting.
    pub const BLOB_READ_POST_CAPTURE: &str = "blob_read.post_capture";
    /// A change-feed poll has captured its cut, but has not reopened any
    /// commit's per-branch manifest snapshot yet. Tests delete and recreate a
    /// named branch here to prove the poll fails closed rather than emitting the
    /// replacement branch's rows under the captured commit's label.
    pub const CHANGE_FEED_POST_CAPTURE: &str = "change_feed.post_capture";
    /// A change-feed poll has reopened and re-proven a commit's manifest
    /// snapshot, but has not yet opened the per-table datasets it names. Tests
    /// delete and recreate a named branch here to prove the physical table open
    /// re-proves the branch incarnation (via the manifest e_tag) rather than
    /// reading the replacement branch's rows at the same path and version.
    pub const CHANGE_FEED_PRE_TABLE_OPEN: &str = "change_feed.pre_table_open";
    /// Behavior seam (`is_enabled`), not an error injection: simulates a store
    /// whose persisted table version metadata carries no e_tag, so
    /// `open_at_entry_verified` cannot use its e_tag comparison. Tests combine
    /// it with `CHANGE_FEED_PRE_TABLE_OPEN` + a branch delete/recreate to prove
    /// the LOGICAL post-open head re-prove still refuses the replacement —
    /// the e_tag is defense-in-depth, not the load-bearing witness.
    pub const CHANGE_FEED_SKIP_ETAG_WITNESS: &str = "change_feed.skip_etag_witness";
    /// A change-feed poll has passed the final post-open logical head witness
    /// for one commit, and is about to plan each interval's emitter. Tests
    /// delete and recreate a named branch here: any live read of the branch's
    /// numeric-path history after this point (the replaceable read — version
    /// manifests sit at numeric paths, unlike UUID-named data and transaction
    /// files) would classify the interval from the REPLACEMENT branch's
    /// transactions and can silently omit the original commit's deletes.
    pub const CHANGE_FEED_POST_HEAD_WITNESS: &str = "change_feed.post_head_witness";
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
    /// Fires past init's commit point — the graph must survive errors
    /// injected here.
    pub const INIT_AFTER_COORDINATOR_INIT: &str = "init.after_coordinator_init";
    pub const INIT_AFTER_SCHEMA_CONTRACT_WRITTEN: &str = "init.after_schema_contract_written";
    pub const INIT_AFTER_SCHEMA_PG_WRITTEN: &str = "init.after_schema_pg_written";
    /// After Lance has durably returned the new `__manifest` Dataset, but
    /// before OmniGraph's create half can acknowledge it. Returning an error
    /// here models a lost object-store acknowledgement and must route through
    /// exact-genesis classification rather than schema cleanup.
    pub const INIT_MANIFEST_CREATE_ACK_LOST: &str = "init.manifest_create_ack_lost";
    /// Before the durable exact-genesis probe used to classify an
    /// acknowledgement-unknown manifest Create. An injected failure proves the
    /// caller preserves schema artifacts when the outcome cannot be observed.
    pub const INIT_MANIFEST_CREATE_PROBE: &str = "init.manifest_create_probe";
    /// After a per-type Lance dataset Create returns success, before graph
    /// initialization can acknowledge it. The graph manifest does not exist
    /// yet, but retry and schema cleanup are unsafe because the table Create
    /// may be durable.
    pub const INIT_TABLE_CREATE_ACK_LOST: &str = "init.table_create_ack_lost";
    /// Inject an indeterminate schema-artifact delete during pre-physical init
    /// cleanup. The original init error must win and the durable claim must be
    /// retained so a delayed delete cannot race another initializer.
    pub const INIT_SCHEMA_CLEANUP_DELETE: &str = "init.schema_cleanup_delete";
    /// The first ordinary post-commit read-back failpoint after the graph's
    /// `__manifest` Create has been positively classified. A crash OR an error
    /// return here must leave an openable graph; init's schema cleanup is
    /// unreachable from this window (issue #495).
    pub const INIT_POST_MANIFEST_CREATE: &str = "init.post_manifest_create";
    /// A read-write bind of a local graph root, before the create-if-absent
    /// probe writes its probe object. Injecting here simulates a filesystem
    /// without hard-link support (issue #453) for both `init` and
    /// read-write `open`.
    pub const LOCAL_CREATE_IF_ABSENT_PROBE: &str = "storage.local_create_if_absent_probe";
    /// The implicit fork-if-missing branch
    /// create completed durably, before any load staging byte is written.
    /// The load "never happened" yet its target branch exists — a failed
    /// load's surviving empty branch.
    pub const LOAD_POST_BRANCH_CREATE_PRE_STAGE: &str = "load.post_branch_create_pre_stage";
    /// Between per-table fragment uploads
    /// inside `stage_all_with_concurrency`. A crash here
    /// leaves a PARTIAL set of staged files across tables with no breadcrumb
    /// — benign by construction (unreferenced, reclaimable by cleanup), which
    /// is exactly what the window lets a universe prove.
    pub const LOAD_BETWEEN_TABLE_STAGES: &str = "load.between_table_stages";
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
    /// After a conditional mutation has executed to a zero-effect result, but
    /// before it acquires the branch gate and revalidates the caller's graph
    /// head. This pins the linearization point for successful no-op CAS calls.
    pub const MUTATION_POST_NO_EFFECT_PRE_GATE: &str = "mutation.post_no_effect_pre_gate";
    pub const MUTATION_POST_FINALIZE_PRE_PUBLISHER: &str = "mutation.post_finalize_pre_publisher";
    /// A stale live read has opened and decoded a replacement manifest whose
    /// exact branch-head row is absent, but has not yet decoded the inherited
    /// lineage fallback. Failure here must leave the old coordinator coherent.
    pub const READ_REFRESH_POST_STATE_PRE_LINEAGE: &str = "read.refresh_post_state_pre_lineage";
    /// Open owns the schema gate and is about to read source/IR/state as one
    /// catalog view.
    pub const OPEN_BEFORE_SCHEMA_CONTRACT_READ: &str = "open.before_schema_contract_read";
    pub const OPTIMIZE_BEFORE_COMPACT: &str = "optimize.before_compact";
    pub const OPTIMIZE_INJECT_REINDEX_CONFLICT: &str = "optimize.inject_reindex_conflict";
    /// After Optimize captures its authority token, before the schema -> main
    /// -> table gates and the revalidation that consumes it. Tests advance the
    /// graph in this window and prove Optimize refuses rather than planning
    /// against authority that has already moved.
    pub const OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES: &str =
        "optimize.post_authority_capture_pre_gates";
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
    pub const RECOVERY_POST_SIDECAR_LIST_PRE_READ: &str = "recovery.post_sidecar_list_pre_read";
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
    /// The EXACT registry this guard configured. Teardown must use the
    /// same instance: resolving `registry()` again at drop time would,
    /// after a concurrent `set_registry`, remove from the new registry
    /// and leak the failpoint in the original.
    registry: Arc<FailPointRegistry>,
}

#[cfg(feature = "failpoints")]
impl ScopedFailPoint {
    pub fn new(name: &str, action: &str) -> Self {
        let registry = registry();
        fail_parallel::cfg(registry.clone(), name, action).expect("configure failpoint");
        Self {
            name: name.to_string(),
            registry,
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
        let registry = registry();
        fail_parallel::cfg_callback(registry.clone(), name, callback)
            .expect("configure callback failpoint");
        Self {
            name: name.to_string(),
            registry,
        }
    }
}

#[cfg(feature = "failpoints")]
impl Drop for ScopedFailPoint {
    fn drop(&mut self) {
        fail_parallel::remove(self.registry.clone(), &self.name);
    }
}
