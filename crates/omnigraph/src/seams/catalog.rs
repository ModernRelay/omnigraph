//! The compile-checked index of every decision seam in this crate. Each
//! static is declared beside the site it guards, so the compiler records
//! where it is; this module re-exports every one under the string a case
//! file uses, and `ALL` lists them for the runner and the guard.

use omnigraph_seams::DecideSeam;

omnigraph_seams::catalog! {
    crate::blob::BLOB_READ_POST_CAPTURE,
    crate::branch_control::BRANCH_CREATE_POST_NATIVE,
    crate::branch_control::BRANCH_DELETE_POST_NATIVE,
    crate::changes::enumerate::CHANGE_FEED_POST_HEAD_WITNESS,
    crate::changes::enumerate::CHANGE_FEED_PRE_TABLE_OPEN,
    crate::db::graph_coordinator::GRAPH_PUBLISH_AFTER_MANIFEST_COMMIT,
    crate::db::graph_coordinator::GRAPH_PUBLISH_BEFORE_COMMIT_APPEND,
    crate::db::manifest::READ_REFRESH_POST_STATE_PRE_LINEAGE,
    crate::db::manifest::graph::INIT_MANIFEST_CREATE_POST_NATIVE,
    crate::db::manifest::graph::INIT_MANIFEST_CREATE_PROBE,
    crate::db::manifest::graph::INIT_POST_MANIFEST_CREATE,
    crate::db::manifest::graph::INIT_TABLE_CREATE_POST_NATIVE,
    crate::db::manifest::publisher::PUBLISH_LOAD_STATE,
    crate::db::manifest::upgrade::UPGRADE_AFTER_ACTIVATION,
    crate::db::manifest::upgrade::UPGRADE_AFTER_BRANCH,
    crate::db::manifest::upgrade::UPGRADE_AFTER_FENCE,
    crate::db::manifest::upgrade::UPGRADE_AFTER_STAGE,
    crate::db::manifest::upgrade::UPGRADE_BEFORE_ACTIVATION,
    crate::db::omnigraph::BRANCH_CONTROL_PRE_GATES,
    crate::db::omnigraph::BRANCH_DELETE_POST_TABLE_GATES,
    crate::db::omnigraph::CHANGE_FEED_POST_CAPTURE,
    crate::db::omnigraph::INIT_AFTER_COORDINATOR_INIT,
    crate::db::omnigraph::INIT_AFTER_SCHEMA_CONTRACT_WRITTEN,
    crate::db::omnigraph::INIT_AFTER_SCHEMA_PG_WRITTEN,
    crate::db::omnigraph::INIT_SCHEMA_CLEANUP_DELETE,
    crate::db::omnigraph::LOCAL_CREATE_IF_ABSENT_PROBE,
    crate::db::omnigraph::OPEN_BEFORE_SCHEMA_CONTRACT_READ,
    crate::db::omnigraph::SCHEMA_RELOAD_BEFORE_CONTRACT_READ,
    crate::db::omnigraph::optimize::CLASSIFY_FRESH_READ,
    crate::db::omnigraph::optimize::CLEANUP_RECONCILE_FORK,
    crate::db::omnigraph::optimize::CLEANUP_RESOLVE_BRANCH_SNAPSHOT,
    crate::db::omnigraph::optimize::CLEANUP_PRE_GATES,
    crate::db::omnigraph::optimize::CLEANUP_TABLE_GC,
    crate::db::omnigraph::optimize::OPTIMIZE_BEFORE_COMPACT,
    crate::db::omnigraph::optimize::OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES,
    crate::db::omnigraph::optimize::OPTIMIZE_POST_PUBLISH_PRE_PROMOTION,
    crate::db::omnigraph::optimize::OPTIMIZE_POST_TABLE_EFFECT,
    crate::db::omnigraph::optimize::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    crate::db::omnigraph::schema_apply::SCHEMA_APPLY_AFTER_MANIFEST_COMMIT,
    crate::db::omnigraph::schema_apply::SCHEMA_APPLY_AFTER_STAGING_WRITE,
    crate::db::omnigraph::schema_apply::SCHEMA_APPLY_BEFORE_STAGING_WRITE,
    crate::db::omnigraph::schema_apply::SCHEMA_APPLY_POST_LOCK_PRE_EFFECT,
    crate::db::omnigraph::schema_apply::SCHEMA_APPLY_POST_PUBLISH_PRE_PROMOTION,
    crate::db::omnigraph::schema_apply::SCHEMA_APPLY_POST_TABLE_COMMIT,
    crate::db::omnigraph::table_ops::ENSURE_INDICES_POST_FORK_PRE_COMMIT,
    crate::db::omnigraph::table_ops::ENSURE_INDICES_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    crate::db::omnigraph::table_ops::ENSURE_INDICES_POST_PUBLISH_PRE_PROMOTION,
    crate::db::omnigraph::table_ops::ENSURE_INDICES_POST_STAGE_PRE_COMMIT_BTREE,
    crate::db::omnigraph::table_ops::ENSURE_INDICES_POST_TABLE_EFFECT,
    crate::db::omnigraph::table_ops::FORK_BEFORE_CLASSIFY,
    crate::exec::merge::BRANCH_MERGE_ADOPT_AFTER_APPEND_PRE_UPSERT,
    crate::exec::merge::BRANCH_MERGE_ADOPT_AFTER_UPSERT_PRE_DELETE,
    crate::exec::merge::BRANCH_MERGE_ADOPT_BETWEEN_INSERT_CHUNKS,
    crate::exec::merge::BRANCH_MERGE_BETWEEN_DELETE_CHUNKS,
    crate::exec::merge::BRANCH_MERGE_POST_AUTHORITY_CAPTURE,
    crate::exec::merge::BRANCH_MERGE_POST_CANDIDATE_VALIDATION,
    crate::exec::merge::BRANCH_MERGE_POST_FORK_PRE_COMMIT,
    crate::exec::merge::BRANCH_MERGE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
    crate::exec::merge::BRANCH_MERGE_POST_PUBLISH_PRE_PROMOTION,
    crate::exec::merge::BRANCH_MERGE_POST_TABLE_EFFECT,
    crate::exec::merge::BRANCH_MERGE_REWRITE_AFTER_DELETE_PRE_CONFIRM,
    crate::exec::merge::BRANCH_MERGE_REWRITE_AFTER_INSERT_PRE_UPDATE,
    crate::exec::merge::BRANCH_MERGE_REWRITE_AFTER_MERGE_PRE_DELETE,
    crate::exec::mutation::MUTATION_DELETE_NODE_PRE_PRIMARY_DELETE,
    crate::exec::mutation::MUTATION_POST_FINALIZE_PRE_PUBLISHER,
    crate::exec::mutation::MUTATION_POST_NO_EFFECT_PRE_GATE,
    crate::exec::mutation::MUTATION_POST_PUBLISH_PRE_PROMOTION,
    crate::exec::mutation::MUTATION_POST_STAGE_PRE_EFFECT_GATE,
    crate::exec::staging::LOAD_BETWEEN_TABLE_STAGES,
    crate::exec::staging::MUTATION_POST_FORK_PRE_COMMIT,
    crate::exec::staging::MUTATION_POST_TABLE_COMMIT,
    crate::loader::LOAD_POST_BRANCH_CREATE_PRE_STAGE,
    crate::table_store::CHANGE_FEED_ETAG_WITNESS,
    crate::table_store::FORK_POST_CREATE_PRE_OPEN,
    crate::db::omnigraph::optimize::CLEANUP_PRE_REAP,
    crate::db::omnigraph::promotion::PROMOTION_POST_LANDED,
    crate::table_store::PROMOTION_PRE_REPLAY,
}

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
