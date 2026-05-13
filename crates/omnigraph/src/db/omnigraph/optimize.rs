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

/// Whether the installed Lance can compact a dataset that contains blob
/// columns. `false` today: Lance `compact_files` forces
/// `BlobHandling::AllBinary` on the read side, and the blob-v2 struct decoder
/// mis-counts columns ("there were more fields in the schema than provided
/// column indices"), failing even a pristine uniform-V2_2 multi-fragment blob
/// table. Reads are unaffected (queries use descriptor handling).
///
/// While `false`, [`optimize_all_tables`] skips blob-bearing tables and reports
/// [`SkipReason::BlobColumnsUnsupportedByLance`] instead of aborting the whole
/// sweep. Flip to `true` once the upstream Lance fix ships — the
/// `lance_surface_guards.rs::compact_files_still_fails_on_blob_columns` guard
/// turns red on that bump and forces this flip. Tracked in `docs/dev/lance.md`.
const LANCE_SUPPORTS_BLOB_COMPACTION: bool = false;

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

/// Why `optimize` did not compact a table. Typed so callers branch on the
/// reason rather than sniffing a string. One variant today, gated by
/// [`LANCE_SUPPORTS_BLOB_COMPACTION`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// The table has one or more `Blob` columns. Lance `compact_files` forces
    /// `BlobHandling::AllBinary`, which mis-decodes blob-v2 columns; see
    /// [`LANCE_SUPPORTS_BLOB_COMPACTION`] and `docs/dev/lance.md`.
    BlobColumnsUnsupportedByLance,
}

impl SkipReason {
    /// Stable machine-readable token for serialized output (e.g. CLI `--json`).
    /// Once emitted this is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::BlobColumnsUnsupportedByLance => "blob_columns_unsupported_by_lance",
        }
    }
}

impl std::fmt::Display for SkipReason {
    /// Human-readable reason for CLI and log output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            SkipReason::BlobColumnsUnsupportedByLance => {
                "blob columns — Lance compaction unsupported"
            }
        };
        f.write_str(msg)
    }
}

/// Per-table outcome of `optimize_all_tables`. This is a returned result type,
/// not built by callers, so it is `#[non_exhaustive]`: future fields stay
/// non-breaking and downstream code reads fields rather than constructing it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableOptimizeStats {
    pub table_key: String,
    /// Number of source fragments that were rewritten by Lance.
    pub fragments_removed: usize,
    /// Number of new, larger fragments Lance produced.
    pub fragments_added: usize,
    /// Did this table get a new Lance manifest version from the compaction?
    pub committed: bool,
    /// `Some(reason)` if this table was deliberately not compacted. When set,
    /// `fragments_removed == 0`, `fragments_added == 0`, and `!committed`.
    pub skipped: Option<SkipReason>,
}

impl TableOptimizeStats {
    /// Stat for a table that Lance actually compacted.
    fn compacted(table_key: String, metrics: &CompactionMetrics, committed: bool) -> Self {
        Self {
            table_key,
            fragments_removed: metrics.fragments_removed,
            fragments_added: metrics.fragments_added,
            committed,
            skipped: None,
        }
    }

    /// Stat for a table that was deliberately skipped (compaction not attempted).
    fn skipped(table_key: String, reason: SkipReason) -> Self {
        Self {
            table_key,
            fragments_removed: 0,
            fragments_added: 0,
            committed: false,
            skipped: Some(reason),
        }
    }
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

    // Compute per-table state (path + whether it has blob columns) up front, in
    // a scope that drops the catalog handle before the async stream starts.
    let table_tasks: Vec<(String, String, bool)> = {
        let catalog = db.catalog();
        let mut tasks = Vec::new();
        for table_key in all_table_keys(&catalog) {
            let Some(entry) = snapshot.entry(&table_key) else {
                continue;
            };
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            let has_blob = !blob_properties_for_table_key(&catalog, &table_key)?.is_empty();
            tasks.push((table_key, full_path, has_blob));
        }
        tasks
    };

    if table_tasks.is_empty() {
        return Ok(Vec::new());
    }

    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);
    let storage = db.storage();

    let stats: Vec<Result<TableOptimizeStats>> = futures::stream::iter(table_tasks.into_iter())
        .map(|(table_key, full_path, has_blob)| async move {
            // Lance `compact_files` mis-decodes blob-v2 columns under the forced
            // `BlobHandling::AllBinary` read (see LANCE_SUPPORTS_BLOB_COMPACTION).
            // Skip blob-bearing tables and report it rather than aborting the
            // whole sweep — the other tables still compact.
            if has_blob && !LANCE_SUPPORTS_BLOB_COMPACTION {
                tracing::warn!(
                    target: "omnigraph::optimize",
                    table = %table_key,
                    "skipping compaction: table has blob columns the current Lance \
                     cannot rewrite (blob-v2 AllBinary decode bug); other tables \
                     unaffected — rerun after the Lance fix",
                );
                return Ok(TableOptimizeStats::skipped(
                    table_key,
                    SkipReason::BlobColumnsUnsupportedByLance,
                ));
            }
            // `compact_files` is a Lance-only maintenance API that needs
            // `&mut Dataset`. The `TableStorage` trait deliberately does
            // not surface it (the staged-write invariant covers writes;
            // compaction is a separate concern). Unwrap the opaque
            // `SnapshotHandle` via `into_dataset()` (`pub(crate)` and
            // gated to the maintenance path).
            let handle = storage
                .open_dataset_head_for_write(&table_key, &full_path, None)
                .await?;
            let mut ds = handle.into_dataset();
            let version_before = ds.version().version;
            let metrics: CompactionMetrics =
                compact_files(&mut ds, CompactionOptions::default(), None)
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            let version_after = ds.version().version;
            Ok(TableOptimizeStats::compacted(
                table_key,
                &metrics,
                version_after != version_before,
            ))
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
    if !reconciled.failures.is_empty() {
        tracing::warn!(
            count = reconciled.failures.len(),
            failures = ?reconciled.failures,
            "cleanup could not reconcile some orphaned forks; will retry next cleanup"
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
    let storage = db.storage();

    // Fault-isolated per table: a single table's GC failure is recorded on its
    // stats row (`error: Some`) and logged, never aborting the healthy tables.
    // cleanup is the convergence backstop, so it must do as much as it can and
    // converge on re-run rather than fail wholesale (invariant 13).
    let results: Vec<TableCleanupStats> = futures::stream::iter(table_tasks.into_iter())
        .map(|(table_key, full_path)| async move {
            let outcome: Result<RemovalStats> = async {
                crate::failpoints::maybe_fail("cleanup.table_gc")?;
                // `cleanup_old_versions` is a Lance-only maintenance API not
                // surfaced through `TableStorage` — see the optimize path
                // above for the same rationale. Unwrap via `into_dataset()`.
                let handle = storage
                    .open_dataset_head_for_write(&table_key, &full_path, None)
                    .await?;
                let ds = handle.into_dataset();
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
                lance::dataset::cleanup::cleanup_old_versions(&ds, policy)
                    .await
                    .map_err(|e| OmniError::Lance(e.to_string()))
            }
            .await;
            match outcome {
                Ok(removed) => TableCleanupStats {
                    table_key,
                    bytes_removed: removed.bytes_removed,
                    old_versions_removed: removed.old_versions,
                    error: None,
                },
                Err(err) => {
                    tracing::warn!(
                        target: "omnigraph::cleanup",
                        table = %table_key,
                        error = %err,
                        "version GC failed for table; other tables unaffected",
                    );
                    TableCleanupStats {
                        table_key,
                        bytes_removed: 0,
                        old_versions_removed: 0,
                        error: Some(err.to_string()),
                    }
                }
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    Ok(results)
}

/// Outcome of [`reconcile_orphaned_branches`]: the `(owner, branch)` pairs
/// reclaimed and the `(owner, error)` pairs that failed, where `owner` is a
/// table key (e.g. `node:Person`) or `"_graph_commits"`. Per-owner failures are
/// isolated and recorded here, not propagated — the next reconcile converges.
#[derive(Debug, Clone, Default)]
pub struct BranchReconcileStats {
    pub reclaimed: Vec<(String, String)>,
    pub failures: Vec<(String, String)>,
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

    // Per-table fault isolation: one table's transient failure is recorded and
    // logged, never aborting the rest of the sweep.
    let storage = db.storage();
    for (table_key, full_path) in table_targets {
        let listed = match storage.list_branches(&full_path).await {
            Ok(listed) => listed,
            Err(err) => {
                tracing::warn!(
                    target: "omnigraph::cleanup",
                    table = %table_key,
                    error = %err,
                    "listing branches failed during reconcile; skipping table",
                );
                stats.failures.push((table_key.clone(), err.to_string()));
                continue;
            }
        };
        for branch in orphan_branches(listed, &keep) {
            let outcome = match crate::failpoints::maybe_fail("cleanup.reconcile_fork") {
                Ok(()) => storage.force_delete_branch(&full_path, &branch).await,
                Err(injected) => Err(injected),
            };
            match outcome {
                Ok(()) => stats.reclaimed.push((table_key.clone(), branch)),
                Err(err) => {
                    tracing::warn!(
                        target: "omnigraph::cleanup",
                        table = %table_key,
                        branch = %branch,
                        error = %err,
                        "reclaiming orphaned fork failed; will retry next cleanup",
                    );
                    stats.failures.push((table_key.clone(), err.to_string()));
                }
            }
        }
    }

    // Commit-graph orphans (best-effort: the dataset may not exist on a graph
    // that has never committed; any failure is isolated and retried next time).
    if let Err(err) = reconcile_commit_graph_orphans(db, &keep, &mut stats).await {
        tracing::warn!(
            target: "omnigraph::cleanup",
            error = %err,
            "commit-graph orphan reconcile failed; will retry next cleanup",
        );
        stats.failures.push(("_graph_commits".to_string(), err.to_string()));
    }

    Ok(stats)
}

/// Commit-graph half of [`reconcile_orphaned_branches`], split out so its
/// errors can be isolated. Returns `Ok` when the commit-graph dataset is absent.
async fn reconcile_commit_graph_orphans(
    db: &Omnigraph,
    keep: &std::collections::HashSet<String>,
    stats: &mut BranchReconcileStats,
) -> Result<()> {
    let commits_uri = crate::db::commit_graph::graph_commits_uri(db.root_uri());
    if !db.storage_adapter().exists(&commits_uri).await? {
        return Ok(());
    }
    let mut commit_graph = crate::db::commit_graph::CommitGraph::open(db.root_uri()).await?;
    for branch in orphan_branches(commit_graph.list_branches().await?, keep) {
        match commit_graph.force_delete_branch(&branch).await {
            Ok(()) => stats.reclaimed.push(("_graph_commits".to_string(), branch)),
            Err(err) => {
                tracing::warn!(
                    target: "omnigraph::cleanup",
                    branch = %branch,
                    error = %err,
                    "reclaiming orphaned commit-graph branch failed; will retry next cleanup",
                );
                stats.failures.push(("_graph_commits".to_string(), err.to_string()));
            }
        }
    }
    Ok(())
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
