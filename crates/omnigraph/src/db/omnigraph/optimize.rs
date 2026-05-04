//! Lance compaction + version cleanup exposed at the graph level.
//!
//! Lance accumulates many small `.lance` fragment files per table over the
//! life of a repo: each `write`, `load`, and `change` op appends one or more
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

/// Per-table outcome of `cleanup_all_tables`.
#[derive(Debug, Clone)]
pub struct TableCleanupStats {
    pub table_key: String,
    pub bytes_removed: u64,
    pub old_versions_removed: u64,
}

/// Run Lance `compact_files` on every node + edge table on `main`.
/// Tables run in parallel (bounded concurrency).
pub async fn optimize_all_tables(db: &mut Omnigraph) -> Result<Vec<TableOptimizeStats>> {
    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("optimize").await?;

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;

    let table_tasks: Vec<_> = all_table_keys(&db.catalog)
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

    let before_timestamp = options.older_than.map(|d| Utc::now() - d);
    let keep_versions = options.keep_versions;

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;

    let table_tasks: Vec<_> = all_table_keys(&db.catalog)
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
            })
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    results.into_iter().collect()
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
