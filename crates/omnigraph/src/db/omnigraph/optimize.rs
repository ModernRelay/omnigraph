//! Lance compaction + version cleanup exposed at the graph level.
//!
//! Lance accumulates many small `.lance` fragment files per table over the
//! life of a graph: each `write`, `load`, and `change` op appends one or more
//! fragments and a new manifest. Over long timescales this hurts open times
//! and S3 object counts without improving anything.
//!
//! Two dials:
//!
//! * `optimize_all_tables` — Lance `compact_files` on every table. Rewrites
//!   small fragments into fewer large ones. Non-destructive (creates a new
//!   version; old fragments remain reachable via older manifest versions).
//! * `cleanup_all_tables` — Lance `cleanup_old_versions` on every table.
//!   Removes manifests (and their unique fragments) older than the configured
//!   retention. Destructive to version history — callers should gate this
//!   behind an explicit confirm flag at the CLI layer.
//!
//! Both walk every node + edge table on the `main` branch. Run branches
//! are ephemeral by design so we do not optimize them.

use std::time::Duration;

use chrono::Utc;
use futures::stream::StreamExt;
use lance::dataset::cleanup::{CleanupPolicy, RemovalStats};
use lance::dataset::optimize::{CompactionMetrics, CompactionOptions, compact_files};

use super::*;

/// How many tables to optimize/cleanup concurrently. Each hits a separate
/// Lance dataset so there is no shared state; the bound is there to avoid
/// flooding the runtime and the S3 connection pool.
const DEFAULT_MAINT_CONCURRENCY: usize = 8;

fn maint_concurrency() -> usize {
    std::env::var("OMNIGRAPH_MAINTENANCE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(DEFAULT_MAINT_CONCURRENCY)
}

/// Retention knobs for [`cleanup_all_tables`]. At least one must be set or
/// nothing is cleaned. If both are set, Lance applies them as AND (a manifest
/// is kept if it satisfies either — i.e. only manifests older than BOTH the
/// time cutoff AND the version cutoff are removed).
#[derive(Debug, Clone, Default)]
pub struct CleanupPolicyOptions {
    /// Keep this many most-recent versions per table.
    pub keep_versions: Option<u32>,
    /// Only remove versions older than this duration.
    pub older_than: Option<Duration>,
}

/// Per-table outcome of `optimize_all_tables`.
#[derive(Debug, Clone)]
pub struct TableOptimizeStats {
    pub table_key: String,
    /// Number of source fragments that were rewritten by Lance.
    pub fragments_removed: usize,
    /// Number of new, larger fragments Lance produced.
    pub fragments_added: usize,
    /// Did this table get a new Lance manifest version from the compaction?
    pub committed: bool,
}

/// Per-table outcome of `cleanup_all_tables`. `error` is `Some` when this
/// table's version GC failed; cleanup is fault-isolated per table, so a single
/// table's failure is recorded here rather than aborting the whole sweep.
#[derive(Debug, Clone)]
pub struct TableCleanupStats {
    pub table_key: String,
    pub bytes_removed: u64,
    pub old_versions_removed: u64,
    pub error: Option<String>,
}

/// Run Lance `compact_files` on every node + edge table on `main`.
/// Tables run in parallel (bounded concurrency).
pub async fn optimize_all_tables(db: &Omnigraph) -> Result<Vec<TableOptimizeStats>> {
    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("optimize").await?;

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;

    let table_tasks: Vec<_> = all_table_keys(&db.catalog())
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(&table_key)?;
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            Some((table_key, full_path))
        })
        .collect();

    if table_tasks.is_empty() {
        return Ok(Vec::new());
    }

    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);
    let table_store = &db.table_store;

    let stats: Vec<Result<TableOptimizeStats>> = futures::stream::iter(table_tasks.into_iter())
        .map(|(table_key, full_path)| async move {
            let mut ds = table_store
                .open_dataset_head_for_write(&table_key, &full_path, None)
                .await?;
            let version_before = ds.version().version;
            let metrics: CompactionMetrics =
                compact_files(&mut ds, CompactionOptions::default(), None)
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            let version_after = ds.version().version;
            Ok(TableOptimizeStats {
                table_key,
                fragments_removed: metrics.fragments_removed,
                fragments_added: metrics.fragments_added,
                committed: version_after != version_before,
            })
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    stats.into_iter().collect()
}

/// Run Lance `cleanup_old_versions` on every node + edge table on `main`,
/// using [`CleanupPolicyOptions`]. The latest manifest is always preserved
/// regardless (Lance invariant).
pub async fn cleanup_all_tables(
    db: &mut Omnigraph,
    options: CleanupPolicyOptions,
) -> Result<Vec<TableCleanupStats>> {
    if options.keep_versions.is_none() && options.older_than.is_none() {
        return Err(OmniError::manifest(
            "cleanup requires at least one of keep_versions or older_than",
        ));
    }

    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("cleanup").await?;

    // Reclaim orphaned branch forks (from an incomplete prior `branch_delete`)
    // before version GC. Authority-derived and idempotent; the eager
    // best-effort reclaim in `branch_delete` covers the common case, this is
    // the guaranteed backstop. Logged for observability.
    let reconciled = reconcile_orphaned_branches(db).await?;
    if !reconciled.reclaimed.is_empty() {
        tracing::info!(
            count = reconciled.reclaimed.len(),
            reclaimed = ?reconciled.reclaimed,
            "cleanup reconciled orphaned branch forks"
        );
    }

    let before_timestamp = options.older_than.map(|d| Utc::now() - d);
    let keep_versions = options.keep_versions;

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;

    let table_tasks: Vec<_> = all_table_keys(&db.catalog())
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(&table_key)?;
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            Some((table_key, full_path))
        })
        .collect();

    if table_tasks.is_empty() {
        return Ok(Vec::new());
    }

    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);
    let table_store = &db.table_store;

    let results: Vec<Result<TableCleanupStats>> = futures::stream::iter(table_tasks.into_iter())
        .map(|(table_key, full_path)| async move {
            crate::failpoints::maybe_fail("cleanup.table_gc")?;
            let ds = table_store
                .open_dataset_head_for_write(&table_key, &full_path, None)
                .await?;
            let before_version = keep_versions
                .map(|n| ds.version().version.saturating_sub(n as u64))
                .filter(|v| *v > 0);
            let policy = CleanupPolicy {
                before_timestamp,
                before_version,
                delete_unverified: false,
                error_if_tagged_old_versions: false,
                clean_referenced_branches: false,
                delete_rate_limit: None,
            };
            let removed: RemovalStats = lance::dataset::cleanup::cleanup_old_versions(&ds, policy)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            Ok(TableCleanupStats {
                table_key,
                bytes_removed: removed.bytes_removed,
                old_versions_removed: removed.old_versions,
                error: None,
            })
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    results.into_iter().collect()
}

/// Outcome of [`reconcile_orphaned_branches`]: the `(owner, branch)` pairs
/// reclaimed, where `owner` is a table key (e.g. `node:Person`) or
/// `"_graph_commits"`.
#[derive(Debug, Clone, Default)]
pub struct BranchReconcileStats {
    pub reclaimed: Vec<(String, String)>,
}

/// Drop every per-table and commit-graph Lance branch that the manifest no
/// longer references.
///
/// Orphaned forks arise when a `branch_delete` flips the manifest authority
/// (atomic) but a downstream best-effort reclaim does not complete. They are
/// unreachable through any snapshot — no manifest entry can name them — yet
/// they pin their `tree/{branch}/` storage and can block reusing the branch
/// name. This is the guaranteed convergence backstop: it is idempotent and
/// derived purely from the manifest authority, so it no-ops once everything is
/// reconciled, and it would harmlessly find nothing if a future Lance atomic
/// multi-dataset branch op prevented orphans from forming.
///
/// The keep-set is the full (unfiltered) manifest branch list, so system
/// branches' forks are never reclaimed; `main`/default is not a named Lance
/// branch and so is never a candidate. Referencing children are dropped before
/// parents (Lance refuses to delete a referenced parent) by ordering longest
/// branch names first.
pub async fn reconcile_orphaned_branches(db: &Omnigraph) -> Result<BranchReconcileStats> {
    use std::collections::HashSet;

    let keep: HashSet<String> = db
        .coordinator
        .read()
        .await
        .all_branches()
        .await?
        .into_iter()
        .collect();

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;
    let table_targets: Vec<(String, String)> = all_table_keys(&db.catalog())
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(&table_key)?;
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            Some((table_key, full_path))
        })
        .collect();

    let mut stats = BranchReconcileStats::default();

    for (table_key, full_path) in table_targets {
        let mut orphans = orphan_branches(db.table_store.list_branches(&full_path).await?, &keep);
        for branch in orphans.drain(..) {
            db.table_store
                .force_delete_branch(&full_path, &branch)
                .await?;
            stats.reclaimed.push((table_key.clone(), branch));
        }
    }

    // Commit-graph orphans (best-effort: the dataset may not exist on a graph
    // that has never committed).
    let commits_uri = crate::db::commit_graph::graph_commits_uri(db.root_uri());
    if db.storage_adapter().exists(&commits_uri).await? {
        let mut commit_graph = crate::db::commit_graph::CommitGraph::open(db.root_uri()).await?;
        let mut orphans = orphan_branches(commit_graph.list_branches().await?, &keep);
        for branch in orphans.drain(..) {
            commit_graph.force_delete_branch(&branch).await?;
            stats.reclaimed.push(("_graph_commits".to_string(), branch));
        }
    }

    Ok(stats)
}

/// Filter `present` Lance branches down to those absent from the manifest
/// `keep` set, ordered children-before-parents (longest name first) so Lance's
/// referenced-parent `RefConflict` cannot block reclamation.
fn orphan_branches(present: Vec<String>, keep: &std::collections::HashSet<String>) -> Vec<String> {
    let mut orphans: Vec<String> = present
        .into_iter()
        .filter(|branch| !keep.contains(branch))
        .collect();
    orphans.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    orphans
}

fn all_table_keys(catalog: &omnigraph_compiler::catalog::Catalog) -> Vec<String> {
    let mut keys: Vec<String> = catalog
        .node_types
        .keys()
        .map(|n| format!("node:{}", n))
        .chain(catalog.edge_types.keys().map(|n| format!("edge:{}", n)))
        .collect();
    keys.sort();
    keys
}
