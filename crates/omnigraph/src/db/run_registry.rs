// The Run state machine has been removed. Mutations now write directly
// to target tables and use the publisher's `expected_table_versions`
// CAS for cross-table OCC; `__run__<id>` staging branches and the
// `_graph_runs.lance` state machine no longer exist.
//
// What remains is the branch-name predicate, kept as a defense-in-depth
// guard against users naming a public branch `__run__*`. A future
// production sweep of legacy `_graph_runs.lance` rows and stale
// `__run__*` branches will let this predicate (and this file) go too.

pub(crate) const INTERNAL_RUN_BRANCH_PREFIX: &str = "__run__";

pub(crate) fn is_internal_run_branch(name: &str) -> bool {
    name.trim_start_matches('/')
        .starts_with(INTERNAL_RUN_BRANCH_PREFIX)
}
