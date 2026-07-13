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
//!   small fragments into fewer large ones, then **publishes the compacted
//!   versions together in one `__manifest` batch** so each `table_version`
//!   tracks the compacted Lance HEAD (reads pin the manifest version, so without
//!   the publish compaction would be invisible to readers and would break the
//!   HEAD-vs-manifest precondition of schema apply / strict writes). Compaction
//!   is content-preserving (Lance `Operation::Rewrite` "reorganizes data
//!   without semantic modification"), so old fragments remain reachable via
//!   older manifest versions until `cleanup` runs.
//! * `cleanup_all_tables` — Lance `cleanup_old_versions` on every table.
//!   Removes manifests (and their unique fragments) older than the configured
//!   retention, capped at the oldest main-table version inherited by any live
//!   lazy graph branch. Destructive to unreferenced version history — callers
//!   should gate this behind an explicit confirm flag at the CLI layer.
//!
//! Both orchestrate the graph's node + edge datasets from main authority;
//! cleanup preserves both Lance-referenced native branch history and the
//! graph-level lazy-branch references Lance cannot observe.

use std::time::Duration;

use chrono::Utc;
use futures::stream::StreamExt;
use lance::dataset::cleanup::{CleanupPolicy, RemovalStats};
use lance::dataset::optimize::{
    CompactionMetrics, CompactionOptions, compact_files, plan_compaction,
};
use lance::index::DatasetIndexExt;
use lance_index::optimize::OptimizeOptions;

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

/// Why `optimize` did not compact a table. Typed so callers branch on the
/// reason rather than sniffing a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// The Lance dataset HEAD is ahead of the version recorded in
    /// `__manifest`, and no recovery sidecar covers that movement. `optimize`
    /// cannot infer whether the drift is benign maintenance or an external
    /// semantic write, so it leaves the table untouched and points operators at
    /// explicit `repair`.
    DriftNeedsRepair,
}

impl SkipReason {
    /// Stable machine-readable token for serialized output (e.g. CLI `--json`).
    /// Once emitted this is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::DriftNeedsRepair => "drift_needs_repair",
        }
    }
}

impl std::fmt::Display for SkipReason {
    /// Human-readable reason for CLI and log output.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            SkipReason::DriftNeedsRepair => "manifest/head drift — run omnigraph repair",
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
    /// Did this table get a new manifest version from the compaction? True when
    /// compaction ran and its compacted version was published to `__manifest`.
    pub committed: bool,
    /// `Some(reason)` if this table was deliberately not compacted. When set,
    /// `fragments_removed == 0`, `fragments_added == 0`, and `!committed`.
    pub skipped: Option<SkipReason>,
    /// Manifest table version observed by optimize for drift skips. `None` for
    /// normal compaction/no-op outcomes.
    pub manifest_version: Option<u64>,
    /// Lance HEAD version observed by optimize for drift skips. `None` for
    /// normal compaction/no-op outcomes.
    pub lance_head_version: Option<u64>,
    /// Declared `@index` columns on this table the reconciler could not build
    /// this run, each with the `reason` (today: a vector column with no
    /// trainable vectors yet). Empty on the common path. Reported, not fatal — a
    /// later `optimize` retries; the `list_indices`/`indisvalid` analog so
    /// operators can see which index is pending and why.
    pub pending_indexes: Vec<super::PendingIndex>,
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
            manifest_version: None,
            lance_head_version: None,
            pending_indexes: Vec::new(),
        }
    }

    /// Stat for a table skipped because the manifest and Lance HEAD disagree.
    fn skipped_for_drift(
        table_key: String,
        manifest_version: u64,
        lance_head_version: u64,
    ) -> Self {
        Self {
            table_key,
            fragments_removed: 0,
            fragments_added: 0,
            committed: false,
            skipped: Some(SkipReason::DriftNeedsRepair),
            manifest_version: Some(manifest_version),
            lance_head_version: Some(lance_head_version),
            pending_indexes: Vec::new(),
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

struct OptimizeTableTask {
    table_key: String,
    full_path: String,
    expected_version: u64,
}

struct PreparedOptimizeTable {
    table_key: String,
    full_path: String,
    expected_version: u64,
    initial_snapshot: crate::storage_layer::SnapshotHandle,
}

enum OptimizePreparation {
    Work(PreparedOptimizeTable),
    Stat(TableOptimizeStats),
}

struct OptimizeEffectOutcome {
    stat: TableOptimizeStats,
    update: Option<crate::db::SubTableUpdate>,
}

/// Run Lance maintenance across every node + edge table on `main` under one
/// graph visibility envelope. Physical table work remains bounded-parallel,
/// but every productive table shares one recovery sidecar and one monotonic
/// manifest batch publish, so one public Optimize produces at most one graph
/// commit. The final physical `__manifest` compaction remains outside that
/// graph-visible envelope because the internal table is read directly at HEAD.
pub async fn optimize_all_tables(db: &Omnigraph) -> Result<Vec<TableOptimizeStats>> {
    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("optimize").await?;

    // Refuse on an unrecovered graph. A pending recovery sidecar means a failed
    // write left partial state that the open-time sweep must resolve (roll
    // forward/back) first; compacting + publishing a table covered by such a
    // sidecar could commit a partial write the sweep would roll back. Reopen the
    // graph to run recovery, then re-run optimize.
    if !crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter())
        .await?
        .is_empty()
    {
        return Err(OmniError::manifest_conflict(
            "optimize requires a clean recovery state; reopen the graph to run the \
             recovery sweep before optimizing",
        ));
    }
    // Deterministic race seam: the broad fast-path probe above has completed,
    // but main's branch-writer gate is not held yet. A writer may arm recovery
    // in this window; the load-bearing check below runs only after Optimize owns
    // the branch authority every sidecar-enrolled main writer must cross.
    crate::failpoints::maybe_fail(
        crate::failpoints::names::OPTIMIZE_POST_RECOVERY_CHECK_PRE_MAIN_GATE,
    )?;

    // Canonical writer order: schema -> branch -> sorted tables. Planning reads
    // catalog index intent, so it must use an operation-local accepted catalog
    // under the same schema gate as schema apply and the exact RFC-022 writers.
    let schema_gate_key = crate::db::manifest::schema_apply_serial_queue_key();
    let schema_guard = db.write_queue().acquire(&schema_gate_key).await;
    db.refresh_coordinator_only().await?;
    db.ensure_schema_apply_not_locked("optimize").await?;
    let catalog = db.load_accepted_catalog_with_schema_gate_held().await?;

    // Optimize's one visibility point advances main's graph head, so its
    // authority is branch-wide even though physical effects are table-local.
    // Retain main through the final physical-only __manifest compaction so a
    // new main recovery intent cannot arm before raw manifest movement ends.
    let _main_branch_guard = db.write_queue().acquire_branch(None).await;

    let table_keys = all_table_keys(&catalog);
    let queue_keys = table_keys
        .iter()
        .map(|table_key| (table_key.clone(), None))
        .collect::<Vec<_>>();
    let table_guards = db.write_queue().acquire_many(&queue_keys).await;

    // This relist is authoritative: every in-process writer that could have
    // armed a main/global intent crosses one of the gates now held. The entry
    // probe above is only a cheap fast-path/race seam.
    ensure_no_pending_recovery_for_optimize_under_main_gate(db).await?;
    let snapshot = db.fresh_snapshot_for_branch(None).await?;

    let table_tasks = table_keys
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(&table_key)?;
            Some(OptimizeTableTask {
                table_key,
                full_path: format!("{}/{}", db.root_uri, entry.table_path),
                expected_version: entry.table_version,
            })
        })
        .collect::<Vec<_>>();

    // NB: do NOT early-return when `table_tasks` is empty (a schema with no
    // node/edge types) — the internal system tables below must still be compacted.
    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);

    let preparations: Vec<Result<OptimizePreparation>> =
        futures::stream::iter(table_tasks.into_iter())
            .map(|task| {
                let catalog = std::sync::Arc::clone(&catalog);
                async move { prepare_optimize_table(db, catalog.as_ref(), task).await }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

    let mut prepared = Vec::new();
    let mut stats = Vec::new();
    for preparation in preparations {
        match preparation? {
            OptimizePreparation::Work(work) => prepared.push(work),
            OptimizePreparation::Stat(stat) => stats.push(stat),
        }
    }
    prepared.sort_by(|left, right| left.table_key.cmp(&right.table_key));

    if !prepared.is_empty() {
        // Phase A: one durable bounded-v2 intent before the first table HEAD
        // advance. Exact provenance is deferred until Lance has a stable public
        // maintenance-transaction API and recovery has a distributed fence.
        let pins = prepared
            .iter()
            .map(|work| crate::db::manifest::SidecarTablePin {
                table_key: work.table_key.clone(),
                table_path: work.full_path.clone(),
                expected_version: work.expected_version,
                // Lower bound: compact_files may reserve then rewrite, while
                // reindex/config/index work can add more versions.
                post_commit_pin: work.expected_version + 1,
                confirmed_version: None,
                table_branch: None,
            })
            .collect();
        let sidecar = crate::db::manifest::new_sidecar(
            crate::db::manifest::SidecarKind::Optimize,
            None,
            None,
            pins,
        );
        let recovery_handle =
            crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar)
                .await?;

        // Phase B: settle every bounded-parallel task. Never early-cancel
        // siblings: after the shared sidecar is armed, recovery needs the most
        // knowable completed effect set possible.
        let effect_concurrency = maint_concurrency().min(prepared.len()).max(1);
        let effect_results: Vec<Result<OptimizeEffectOutcome>> =
            futures::stream::iter(prepared.into_iter())
                .map(|work| {
                    let catalog = std::sync::Arc::clone(&catalog);
                    async move { apply_optimize_table_effects(db, catalog.as_ref(), work).await }
                })
                .buffer_unordered(effect_concurrency)
                .collect()
                .await;

        let mut outcomes = Vec::new();
        let mut first_error = None;
        for result in effect_results {
            match result {
                Ok(outcome) => outcomes.push(outcome),
                Err(error) if first_error.is_none() => first_error = Some(error),
                Err(_) => {}
            }
        }
        if let Some(error) = first_error {
            return Err(optimize_recovery_required(&recovery_handle, error));
        }

        // One graph-wide Phase-B -> Phase-C crash seam, after every physical
        // effect and before the only graph visibility point.
        if let Err(error) = crate::failpoints::maybe_fail(
            crate::failpoints::names::OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT,
        ) {
            return Err(optimize_recovery_required(&recovery_handle, error));
        }

        let updates = outcomes
            .iter()
            .filter_map(|outcome| outcome.update.clone())
            .collect::<Vec<_>>();
        if let Err(error) = publish_optimize_batch_monotonic(db, &updates).await {
            return Err(optimize_recovery_required(&recovery_handle, error));
        }

        let any_committed = outcomes.iter().any(|outcome| outcome.stat.committed);
        let edge_committed = outcomes
            .iter()
            .any(|outcome| outcome.stat.committed && outcome.stat.table_key.starts_with("edge:"));
        stats.extend(outcomes.into_iter().map(|outcome| outcome.stat));

        // Phase D: the graph-visible postcondition is durable. A failed delete
        // is harmless; legacy recovery recognizes the manifest-aligned stale
        // sidecar and records/cleans it on the next pass.
        if let Err(err) =
            crate::db::manifest::delete_sidecar(&recovery_handle, db.storage_adapter()).await
        {
            tracing::warn!(
                error = %err,
                operation_id = recovery_handle.operation_id.as_str(),
                "graph-wide optimize recovery sidecar cleanup failed; next open will resolve it"
            );
        }

        // Cache invalidation happens once, after the one visibility point. A
        // partial Phase B is deliberately not exposed through runtime caches.
        if any_committed {
            db.runtime_cache.invalidate_all().await;
            if edge_committed {
                db.invalidate_graph_index().await;
            }
        }
    }

    stats.sort_by(|left, right| left.table_key.cmp(&right.table_key));

    // Data-table recovery/publish is finished. Release the sorted table and
    // schema gates before physical internal maintenance; retain main's branch
    // gate through that maintenance to preserve the late-intent barrier.
    drop(table_guards);
    drop(schema_guard);

    // Compact the internal system tables too (RFC-013 step 2). They are not
    // catalog-tracked, so they take a separate, simpler path (`compact_internal_table`):
    // compact in place, no manifest publish, no sidecar. Appended after the
    // data-table stats so the data-table cache invalidation above is computed from
    // data-table stats only; each internal compaction does its own coordinator
    // refresh for cache coherence.
    let mut all = stats.into_iter().map(Ok).collect::<Vec<Result<_>>>();
    // The only internal system table optimize compacts is `__manifest`: it
    // accumulates one fragment per commit (both the table-version rows and the
    // folded-in graph-lineage rows — RFC-013 Phase 7), so a long history leaves
    // an O(history) scan on every read/write probe until it is compacted. Graph
    // lineage no longer has its own datasets (`_graph_commits` /
    // `_graph_commit_actors` are retired), so there is nothing else to compact.
    // `__manifest` is always present (created at init).
    let root = db.root_uri();
    let internal_tables: [(&str, String); 1] =
        [("__manifest", crate::db::manifest::manifest_uri(root))];
    for (table_key, uri) in internal_tables {
        all.push(compact_internal_table(db, table_key, uri).await);
    }

    all.into_iter().collect()
}

/// Pure planning: classify drift/no-work/productive state without advancing
/// any Lance HEAD. The caller holds schema -> main -> all table gates.
async fn prepare_optimize_table(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
    task: OptimizeTableTask,
) -> Result<OptimizePreparation> {
    let snapshot = db
        .storage()
        .open_dataset_head(&task.full_path, None)
        .await?;
    let lance_head_version = snapshot.version();
    if lance_head_version < task.expected_version {
        return Err(OmniError::manifest_internal(format!(
            "table '{}' Lance HEAD version {} is behind manifest version {}",
            task.table_key, lance_head_version, task.expected_version
        )));
    }
    if lance_head_version > task.expected_version {
        tracing::warn!(
            target: "omnigraph::optimize",
            table = %task.table_key,
            manifest_version = task.expected_version,
            lance_head_version,
            "skipping compaction: Lance HEAD is ahead of the manifest; run `omnigraph repair` \
             to classify and publish covered maintenance drift explicitly",
        );
        return Ok(OptimizePreparation::Stat(
            TableOptimizeStats::skipped_for_drift(
                task.table_key,
                task.expected_version,
                lance_head_version,
            ),
        ));
    }

    let options = CompactionOptions::default();
    let will_compact = plan_compaction(snapshot.dataset(), &options)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?
        .num_tasks()
        > 0;
    let needs_reindex = TableStore::has_unindexed_fragments(snapshot.dataset()).await?;
    let index_work = super::table_ops::index_work_status_on_dataset_for_catalog(
        db,
        catalog,
        &task.table_key,
        &snapshot,
    )
    .await?;
    if !will_compact && !needs_reindex && !index_work.needs_commit {
        let mut stat =
            TableOptimizeStats::compacted(task.table_key, &CompactionMetrics::default(), false);
        stat.pending_indexes = index_work.pending;
        return Ok(OptimizePreparation::Stat(stat));
    }

    Ok(OptimizePreparation::Work(PreparedOptimizeTable {
        table_key: task.table_key,
        full_path: task.full_path,
        expected_version: task.expected_version,
        initial_snapshot: snapshot,
    }))
}

/// Apply one productive table's physical maintenance work. This helper owns no
/// locks, sidecar, or graph publish; those are graph-wide responsibilities of
/// `optimize_all_tables`.
async fn apply_optimize_table_effects(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
    work: PreparedOptimizeTable,
) -> Result<OptimizeEffectOutcome> {
    let table_key = work.table_key;
    let full_path = work.full_path;
    let mut initial_snapshot = Some(work.initial_snapshot);

    // Tracks whether one of OUR Phase-B ops (auto-cleanup strip / compact / reindex)
    // already committed and advanced Lance HEAD past the manifest in a prior attempt.
    // Once true, a reopened `lance_head > manifest` is our own sidecar-covered work,
    // NOT external drift — so the drift guard and the no-op early-return must not treat
    // it as such (that would drop our committed work as uncovered drift).
    let mut head_advanced = false;

    // Outer loop: open → plan → Phase B, reopening + re-planning on a retryable
    // Lance conflict. Breaks with the committed snapshot once Phase B succeeds.
    let mut attempt: u32 = 0;
    let (snapshot, metrics, pending_indexes, committed) = loop {
        attempt += 1;

        let selected = match initial_snapshot.take() {
            Some(snapshot) => snapshot,
            None => db.storage().open_dataset_head(&full_path, None).await?,
        };

        // CAS baseline: the table's current manifest version, re-read each attempt
        // (a reopen means the manifest may have advanced).
        let expected_version = db
            .fresh_snapshot_for_branch(None)
            .await?
            .entry(&table_key)
            .map(|e| e.table_version)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;

        let lance_head_version = selected.version();
        if lance_head_version < expected_version {
            return Err(OmniError::manifest_internal(format!(
                "table '{}' Lance HEAD version {} is behind manifest version {}",
                table_key, lance_head_version, expected_version
            )));
        }
        if !head_advanced && lance_head_version > expected_version {
            // Pre-existing EXTERNAL uncovered drift (we have not advanced HEAD yet) —
            // go through explicit repair. Once `head_advanced` is set, a reopened
            // `lance_head > manifest` is our own prior Phase-B commit (sidecar-covered)
            // that the publish below fast-forwards, NOT external drift, so this guard is
            // skipped on those retries.
            return Err(OmniError::manifest_conflict(format!(
                "optimize table '{}' moved after the graph-wide recovery intent armed: \
                 manifest version {}, Lance HEAD {}",
                table_key, expected_version, lance_head_version
            )));
        }

        // Precise "will it compact?" check — `plan_compaction` also accounts for
        // deletion materialization (which can rewrite even a single fragment).
        let options = CompactionOptions::default();
        let plan = plan_compaction(selected.dataset(), &options)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let will_compact = plan.num_tasks() > 0;
        // Even with nothing to compact, the table may still have index work
        // (needs_reindex: rows appended since the index was built; needs_index_create:
        // a declared `@index` whose physical build schema apply deferred, iss-848).
        // Any of the three enters the publish path. If NONE, this is a no-op and must
        // NOT be pinned in a sidecar (a zero-commit pin classifies NoMovement on
        // recovery and rolls back siblings).
        let needs_reindex = TableStore::has_unindexed_fragments(selected.dataset()).await?;
        let index_work = super::table_ops::index_work_status_on_dataset_for_catalog(
            db, catalog, &table_key, &selected,
        )
        .await?;
        let needs_index_create = index_work.needs_commit;
        if !will_compact && !needs_reindex && !needs_index_create {
            if head_advanced {
                // Nothing left to compact, but a prior attempt already advanced HEAD
                // (e.g. the strip committed, then compaction conflicted, and the reopen
                // is now already compacted). Publish that committed work instead of
                // dropping it as uncovered drift.
                break (
                    selected,
                    CompactionMetrics::default(),
                    index_work.pending,
                    true,
                );
            }
            let mut stat = TableOptimizeStats::compacted(
                table_key.clone(),
                &CompactionMetrics::default(),
                false,
            );
            stat.pending_indexes = index_work.pending;
            return Ok(OptimizeEffectOutcome { stat, update: None });
        }

        // Test seam: a concurrent (cross-process) writer can interleave here, before
        // any Phase-B commit lands, to exercise the reopen+replan path.
        crate::failpoints::maybe_fail(crate::failpoints::names::OPTIMIZE_BEFORE_COMPACT)?;

        // Phase B: scrub stale auto_cleanup (keeps optimize non-destructive on a
        // graph upgraded from a pre-v7 binary whose `compact_files`/`optimize_indices`
        // commits would otherwise fire Lance's auto-cleanup GC hook), compact,
        // incremental reindex, then materialize declared-but-missing indexes. The
        // maintenance APIs still commit inline; missing-index materialization uses a
        // staged CreateIndex immediately committed under this bounded-v2 sidecar.
        // A retryable Lance conflict
        // here means a concurrent writer preempted an overlapping fragment → reopen at
        // the new HEAD and re-plan. Baseline captured BEFORE the scrub so that if the
        // scrub is the only commit, `committed` still triggers the Phase-C publish.
        let mut ds = selected.into_dataset();
        let version_before = ds.version().version;
        match clear_stale_auto_cleanup_config(&mut ds).await {
            // `true` ⇒ the strip committed and advanced HEAD past the manifest.
            Ok(stripped) => head_advanced |= stripped,
            Err(e) if attempt < COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e) => {
                continue;
            }
            Err(e) => return Err(OmniError::Lance(e.to_string())),
        }
        let metrics: CompactionMetrics = if will_compact {
            match compact_files(&mut ds, options, None).await {
                Ok(m) => {
                    head_advanced = true;
                    m
                }
                Err(e) if attempt < COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e) => {
                    continue;
                }
                Err(e) => return Err(OmniError::Lance(e.to_string())),
            }
        } else {
            CompactionMetrics::default()
        };
        // Test seam: inject one retryable reindex conflict AFTER compaction has
        // committed (so HEAD is already ahead of the manifest from our own work),
        // exercising the own-HEAD (not external) drift classification on the next
        // reopened attempt.
        if crate::failpoints::maybe_fail(crate::failpoints::names::OPTIMIZE_INJECT_REINDEX_CONFLICT)
            .is_err()
            && attempt < COMPACTION_RETRY_BUDGET
        {
            continue;
        }
        match ds.optimize_indices(&OptimizeOptions::default()).await {
            Ok(()) => {}
            Err(e) if attempt < COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e) => {
                continue;
            }
            Err(e) => {
                return Err(OmniError::Lance(format!(
                    "optimize_indices on {}: {}",
                    table_key, e
                )));
            }
        }

        let mut snapshot = crate::storage_layer::SnapshotHandle::new(ds);
        let pending_indexes: Vec<super::PendingIndex> =
            super::table_ops::build_indices_on_dataset_for_catalog(
                db,
                catalog,
                &table_key,
                &mut snapshot,
            )
            .await?;
        // optimize_indices / index build may also have committed (folded fragments,
        // built a deferred index). Any HEAD advance this attempt counts too.
        let version_after = snapshot.version();
        head_advanced |= version_after != version_before;

        break (snapshot, metrics, pending_indexes, head_advanced);
    };

    let mut stat = TableOptimizeStats::compacted(table_key, &metrics, committed);
    stat.pending_indexes = pending_indexes;
    let update = if committed {
        let state = db.storage().table_state(&full_path, &snapshot).await?;
        Some(crate::db::SubTableUpdate {
            table_key: stat.table_key.clone(),
            table_version: state.version,
            table_branch: None,
            row_count: state.row_count,
            version_metadata: state.version_metadata,
        })
    } else {
        None
    };
    Ok(OptimizeEffectOutcome { stat, update })
}

/// Publish every still-needed table pointer in one manifest/lineage CAS. This
/// is maintenance-class OCC, not logical read-set OCC: a current pointer at or
/// beyond the achieved target is already converged and is omitted; remaining
/// pointers use their freshly observed versions as row-level expectations.
async fn publish_optimize_batch_monotonic(
    db: &Omnigraph,
    targets: &[crate::db::SubTableUpdate],
) -> Result<()> {
    if targets.is_empty() {
        return Ok(());
    }

    let mut last_conflict = None;
    for _ in 0..COMPACTION_RETRY_BUDGET {
        let current = db.fresh_snapshot_for_branch(None).await?;
        let mut updates = Vec::with_capacity(targets.len());
        let mut expected = std::collections::HashMap::with_capacity(targets.len());
        for target in targets {
            let entry = current.entry(&target.table_key).ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "optimize target '{}' disappeared before graph-wide publish",
                    target.table_key
                ))
            })?;
            if entry.table_version < target.table_version {
                expected.insert(target.table_key.clone(), entry.table_version);
                updates.push(target.clone());
            }
        }
        if updates.is_empty() {
            return Ok(());
        }

        match db
            .coordinator
            .write()
            .await
            .commit_updates_with_actor_with_expected(&updates, &expected, None)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) if is_retryable_manifest_conflict(&error) => last_conflict = Some(error),
            Err(error) => return Err(error),
        }
    }

    let current = db.fresh_snapshot_for_branch(None).await?;
    if targets.iter().all(|target| {
        current
            .entry(&target.table_key)
            .is_some_and(|entry| entry.table_version >= target.table_version)
    }) {
        return Ok(());
    }
    Err(last_conflict.unwrap_or_else(|| {
        OmniError::manifest_conflict(format!(
            "graph-wide optimize publish exhausted {COMPACTION_RETRY_BUDGET} retries"
        ))
    }))
}

fn optimize_recovery_required(
    handle: &crate::db::manifest::RecoverySidecarHandle,
    error: OmniError,
) -> OmniError {
    OmniError::recovery_required(
        handle.operation_id.clone(),
        format!(
            "graph-wide optimize failed after arming recovery; reopen read-write to converge it: {error}"
        ),
    )
}

/// Final recovery-ownership check for main-branch Optimize.
///
/// The caller must hold main's branch-writer gate and retain it through all data
/// effects and graph-head publishes. The top-level probe stays deliberately
/// conservative and refuses any pre-existing sidecar; this final check rejects
/// every late main-target intent plus graph-global SchemaApply. Table-disjoint
/// intents still overlap because each fixed recovery authority includes the
/// shared `graph_head:main` that Optimize advances.
///
/// This is a process-local gate proof, matching the repository's documented
/// single-writer-process boundary. Separate processes remain governed by Lance
/// OCC and recovery classification; this helper is not a distributed lock.
async fn ensure_no_pending_recovery_for_optimize_under_main_gate(db: &Omnigraph) -> Result<()> {
    let sidecars = crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter()).await?;
    let blocking = sidecars.iter().find(|sidecar| {
        sidecar.writer_kind == crate::db::manifest::SidecarKind::SchemaApply
            || sidecar
                .branch
                .as_deref()
                .filter(|branch| *branch != "main")
                .is_none()
    });
    if let Some(sidecar) = blocking {
        return Err(OmniError::recovery_required(
            sidecar.operation_id.clone(),
            format!(
                "pending {:?} recovery operation on branch '{}' blocks optimize",
                sidecar.writer_kind,
                sidecar.branch.as_deref().unwrap_or("main"),
            ),
        ));
    }
    Ok(())
}

/// Bound on the app-level retry of an internal-table compaction against a
/// concurrent live writer (see [`is_retryable_lance_conflict`]).
const COMPACTION_RETRY_BUDGET: u32 = 5;

/// A Lance commit error that means "a concurrent writer preempted us; reload the
/// dataset and rerun." `compact_files` commits via `commit_compaction` ->
/// `apply_commit` *directly* — unlike the merge-insert path it is NOT wrapped in
/// `execute_with_retry`, so a `Rewrite`-vs-`Merge`/`Update`/`Delete` `check_txn`
/// conflict propagates raw instead of being rebased or converted to
/// `TooMuchWriteContention`. Lance's transaction spec prescribes that the
/// *application* reruns these, which is what `compact_internal_table` does — so a
/// maintenance compaction (a physical op) never fails a live write (a logical op),
/// invariant 7. (`TooMuchWriteContention` is included for the exhausted-retry form
/// some commit paths surface.)
fn is_retryable_lance_conflict(err: &lance::Error) -> bool {
    matches!(
        err,
        lance::Error::RetryableCommitConflict { .. }
            | lance::Error::CommitConflict { .. }
            | lance::Error::TooMuchWriteContention { .. }
    )
}

/// A manifest publish conflict that optimize's monotonic Phase-C loop re-evaluates
/// (re-read the current version, then no-op or fast-forward). Both shapes that reach
/// here are `Conflict`-kind and mean "the manifest moved under us; reconsider," never
/// a lost update: the typed `ExpectedVersionMismatch` (a concurrent writer advanced
/// the table) and the publisher's exhausted row-level CAS (`manifest_conflict`).
fn is_retryable_manifest_conflict(err: &OmniError) -> bool {
    matches!(
        err,
        OmniError::Manifest(m) if m.kind == crate::error::ManifestErrorKind::Conflict
    )
}

/// Remove any stored `lance.auto_cleanup.*` config from a table so compaction
/// stays **non-destructive by construction**. Used by both the internal-table
/// path ([`compact_internal_table`]) and the data-table path
/// ([`apply_optimize_table_effects`]).
///
/// `compact_files` / `optimize_indices` commit with a default `CommitConfig`
/// (`skip_auto_cleanup = false`) and `CompactionOptions` exposes no override, so on
/// a dataset whose stored config has `lance.auto_cleanup.interval` set, the
/// compaction/reindex commit would fire Lance's auto-cleanup hook (version GC) —
/// deletion of old versions, including ones `__manifest` pins for snapshots /
/// time-travel (data tables) or that hold lineage/time-travel state (internal
/// tables). New graphs create tables with `auto_cleanup: None` (`manifest/graph.rs`,
/// `commit_graph.rs`, and the data-table create path) so there is nothing to clear;
/// only pre-`auto_cleanup`-fix *upgraded* graphs carry the config. OmniGraph owns
/// version cleanup explicitly (`cleanup`), so Lance's hook is unwanted regardless —
/// clearing it both makes `optimize` non-destructive and aligns the table with the
/// new-graph posture. The `delete_config_keys` commit itself does not GC: the
/// resulting manifest no longer has the `interval` key, so the post-commit hook is a
/// no-op. Returns whether any config was cleared (it advances Lance HEAD iff so).
/// Recovery coverage differs by caller: the data-table path runs this inside the
/// Optimize sidecar window; the internal-table path needs none (it commits at HEAD
/// and is read at HEAD — the strip is a content-preserving config commit, so a crash
/// leaves the table readable and content-identical, see [`compact_internal_table`]).
async fn clear_stale_auto_cleanup_config(
    ds: &mut lance::Dataset,
) -> std::result::Result<bool, lance::Error> {
    let keys: Vec<String> = ds
        .config()
        .keys()
        .filter(|k| k.starts_with("lance.auto_cleanup."))
        .cloned()
        .collect();
    if keys.is_empty() {
        return Ok(false);
    }
    // Merge-update with `None` values to delete the keys — the non-deprecated
    // replacement for `delete_config_keys` (awaiting the builder merges rather
    // than replacing the whole config map).
    let entries: Vec<(&str, Option<&str>)> = keys.iter().map(|k| (k.as_str(), None)).collect();
    ds.update_config(entries).await?;
    Ok(true)
}

/// Compact the INTERNAL system table (`__manifest`) in place.
///
/// Unlike catalog data tables, the internal tables are not tracked in the
/// `__manifest` (they ARE the manifest / the lineage DAG): readers open them at
/// their latest Lance HEAD, so compaction just advances that HEAD and the next
/// reader transparently observes the compacted version. That makes this path much
/// simpler than [`apply_optimize_table_effects`] — no manifest publish (nothing to publish
/// to), and no recovery sidecar. The sidecar-free claim does NOT rest on
/// single-commit atomicity: `compact_files` can emit a `ReserveFragments` commit
/// before the final `Rewrite` (and the config strip is a separate commit before
/// both), so this advances HEAD over one or more commits. It needs no sidecar
/// because every one of those commits is content-preserving and the table is read
/// at HEAD — a crash at any point leaves the table readable and content-identical,
/// and the next `optimize` re-plans. Internal tables carry no Lance index (only
/// `object_id`'s unenforced-PK schema metadata), so no `optimize_indices`.
///
/// Concurrency: no application lock, but `compact_files` does NOT auto-retry a
/// semantic conflict — its `Operation::Rewrite` commits through `apply_commit`
/// directly (not the merge-insert `execute_with_retry` path), so a `Rewrite`
/// vs concurrent `Update`/`Merge`/`Delete` `check_txn` conflict propagates raw.
/// We own the retry here (see [`is_retryable_lance_conflict`]): on a retryable
/// conflict, reopen at the new HEAD and rerun. A follow-up coordinator `refresh`
/// makes the warm internal-table handles observe the compacted HEAD
/// deterministically (the version probe would also self-heal on the next read).
async fn compact_internal_table(
    db: &Omnigraph,
    table_key: &str,
    uri: String,
) -> Result<TableOptimizeStats> {
    // App-level retry against concurrent live writers. compact_files does NOT
    // auto-retry a Rewrite-vs-live-write conflict (see is_retryable_lance_conflict),
    // so optimize would otherwise fail spuriously on a live graph. On a retryable
    // conflict we re-open at the new HEAD and rerun — the canonical Lance-consumer
    // pattern. Each attempt opens fresh because the conflict means the version moved.
    for attempt in 0..COMPACTION_RETRY_BUDGET {
        let handle = db.storage().open_dataset_head(&uri, None).await?;
        let mut ds = handle.into_dataset();

        // Keep optimize non-destructive by construction (see clear_stale_auto_cleanup_config).
        // Returns whether it committed a config-strip (which advances Lance HEAD).
        let cleared_config = match clear_stale_auto_cleanup_config(&mut ds).await {
            Ok(cleared) => cleared,
            Err(e) => {
                if attempt + 1 < COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e) {
                    continue;
                }
                return Err(OmniError::Lance(e.to_string()));
            }
        };

        let options = CompactionOptions::default();
        let plan = plan_compaction(&ds, &options)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        if plan.num_tasks() == 0 {
            // No compaction work, but a config-strip still advanced HEAD — refresh
            // the warm coordinator handles so they observe it deterministically
            // (same cache-coherence step the successful-compaction path takes
            // below; otherwise they stay pinned until the next version probe).
            if cleared_config {
                db.coordinator.write().await.refresh().await?;
            }
            return Ok(TableOptimizeStats::compacted(
                table_key.to_string(),
                &CompactionMetrics::default(),
                false,
            ));
        }

        match compact_files(&mut ds, options, None).await {
            Ok(metrics) => {
                // Cache coherence: re-open the warm coordinator's internal-table
                // handles at the compacted HEAD (they live in `db.coordinator`, not
                // the data-table `runtime_cache`).
                db.coordinator.write().await.refresh().await?;
                return Ok(TableOptimizeStats::compacted(
                    table_key.to_string(),
                    &metrics,
                    true,
                ));
            }
            Err(e) if attempt + 1 < COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e) => {
                continue;
            }
            Err(e) => return Err(OmniError::Lance(e.to_string())),
        }
    }
    Err(OmniError::manifest_conflict(format!(
        "internal-table compaction of {table_key} exhausted {COMPACTION_RETRY_BUDGET} \
         retries against concurrent writers"
    )))
}

/// Run Lance `cleanup_old_versions` on every node + edge table on `main`,
/// using [`CleanupPolicyOptions`]. The latest manifest is always preserved
/// regardless (Lance invariant), and the requested cutoff is capped at the
/// oldest main-table version inherited by a live lazy graph branch.
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

    // Version GC must never run while recovery still needs exact Lance
    // transaction/version history to prove effect ownership or resume an
    // interrupted compensation. Refuse before orphan reconciliation or any
    // per-table cleanup so this operation is all-or-nothing with respect to the
    // recovery-history floor. A read-write reopen resolves the sidecar first.
    if !crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter())
        .await?
        .is_empty()
    {
        return Err(OmniError::manifest_conflict(
            "cleanup requires a clean recovery state; reopen the graph to run the \
             recovery sweep before garbage-collecting versions",
        ));
    }
    crate::failpoints::maybe_fail(
        crate::failpoints::names::CLEANUP_POST_RECOVERY_CHECK_PRE_GATES,
    )?;

    // Close the empty-check -> GC race. Mutation/load take schema then branch
    // then table gates; current legacy sidecar writers take at least their table
    // gates. Cleanup takes the conservative superset and holds it through every
    // `cleanup_old_versions` call, then performs the authoritative sidecar check
    // under those gates. Without this envelope a writer can arm+commit+fail after
    // the fast check and GC can delete the exact transaction/version history
    // Full recovery needs to prove ownership or Restore.
    let _cleanup_schema_guard = db
        .write_queue()
        .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
        .await;
    db.refresh_coordinator_only().await?;
    db.ensure_schema_apply_not_locked("cleanup").await?;
    let cleanup_catalog = db
        .load_accepted_catalog_with_schema_gate_held()
        .await?;

    // Reclaim orphaned branch forks (from an incomplete prior `branch_delete`)
    // before version GC. Authority-derived and idempotent; the eager
    // best-effort reclaim in `branch_delete` covers the common case, this is
    // the guaranteed backstop. Logged for observability.
    let reconciled = reconcile_orphaned_branches_with_catalog(db, &cleanup_catalog).await?;
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

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;

    let table_tasks: Vec<_> = all_table_keys(&cleanup_catalog)
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(&table_key)?;
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            Some((table_key, full_path))
        })
        .collect();

    // Schema gate stability means no native branch create/delete can change this
    // set between enumeration and acquisition. Include main canonically as None;
    // `all_branches` returns the user-facing "main" spelling.
    let mut graph_branches = db
        .coordinator
        .read()
        .await
        .all_branches()
        .await?
        .into_iter()
        .map(|branch| if branch == "main" { None } else { Some(branch) })
        .collect::<Vec<_>>();
    graph_branches.push(None);
    graph_branches.sort();
    graph_branches.dedup();
    let _cleanup_branch_guards = db.write_queue().acquire_branches(&graph_branches).await;
    let gc_queue_keys = db.table_queue_keys_for_branches(&graph_branches, &cleanup_catalog);
    let _cleanup_table_guards = db.write_queue().acquire_many(&gc_queue_keys).await;

    if !crate::db::manifest::list_sidecars(db.root_uri(), db.storage_adapter())
        .await?
        .is_empty()
    {
        return Err(OmniError::manifest_conflict(
            "cleanup observed a recovery sidecar after acquiring its GC gates; reopen the graph \
             read-write to recover before garbage-collecting versions",
        ));
    }

    // Lance protects versions referenced by its own per-dataset branches, but
    // an OmniGraph branch is lazy: until a table is first written on that
    // branch its manifest entry points directly at an older MAIN version and
    // no Lance branch ref exists on the data table. Resolve every live graph
    // branch from fresh authority while schema + all branch/table gates are
    // held, then cap each main dataset's GC cutoff at its oldest such pin.
    // Main itself participates: its manifest-visible version must open and
    // equal Lance HEAD, so uncovered drift is repaired before cleanup rather
    // than letting HEAD-based GC collect graph-visible authority.
    // Any branch snapshot read failure aborts before the first table GC: an
    // unknown live reference is never evidence that a version is disposable.
    let mut oldest_live_main_version_by_path = std::collections::HashMap::<String, u64>::new();
    for branch_target in &graph_branches {
        if branch_target
            .as_deref()
            .is_some_and(crate::db::is_internal_system_branch)
        {
            continue;
        }
        let branch_label = branch_target.as_deref().unwrap_or("main");
        let branch_snapshot = db
            .fresh_snapshot_for_branch(branch_target.as_deref())
            .await
            .map_err(|err| {
                OmniError::manifest_conflict(format!(
                    "cleanup could not classify live branch '{branch_label}'; refusing version GC: {err}"
                ))
            })?;
        for entry in branch_snapshot
            .entries()
            .filter(|entry| entry.table_branch.is_none())
        {
            // Validate that the exact protected version is still openable
            // before GC starts. This catches pre-existing damage from an older
            // cleanup implementation and keeps the sweep fail-closed instead
            // of deleting unrelated history around an already-broken branch.
            entry.open(db.root_uri(), None).await.map_err(|err| {
                OmniError::manifest_conflict(format!(
                    "cleanup could not classify live branch '{branch_label}' table '{}' at main version {}; refusing version GC: {err}",
                    entry.table_key, entry.table_version
                ))
            })?;
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            if branch_target.is_none() {
                let head = db.storage().open_dataset_head(&full_path, None).await?;
                if head.version() != entry.table_version {
                    return Err(OmniError::manifest_conflict(format!(
                        "cleanup found uncovered HEAD drift for table '{}': manifest version {}, \
                         Lance HEAD {}; run `omnigraph repair` before version GC",
                        entry.table_key,
                        entry.table_version,
                        head.version()
                    )));
                }
            }
            oldest_live_main_version_by_path
                .entry(full_path)
                .and_modify(|oldest| *oldest = (*oldest).min(entry.table_version))
                .or_insert(entry.table_version);
        }
    }

    let before_timestamp = options.older_than.map(|d| Utc::now() - d);
    let keep_versions = options.keep_versions;
    let table_tasks = table_tasks
        .into_iter()
        .map(|(table_key, full_path)| {
            let live_main_floor = oldest_live_main_version_by_path.get(&full_path).copied();
            (table_key, full_path, live_main_floor)
        })
        .collect::<Vec<_>>();

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
        .map(|(table_key, full_path, live_main_floor)| async move {
            let outcome: Result<RemovalStats> = async {
                crate::failpoints::maybe_fail(crate::failpoints::names::CLEANUP_TABLE_GC)?;
                // `cleanup_old_versions` is a Lance-only maintenance API not
                // surfaced through `TableStorage` — see the optimize path
                // above for the same rationale. It only needs a raw read borrow.
                let handle = storage.open_dataset_head(&full_path, None).await?;
                let ds = handle.dataset();
                let requested_before_version = if let Some(keep) = keep_versions {
                    // Lance versions are not safely derivable from HEAD
                    // arithmetic after prior GC. Use the actual ordered
                    // version list so `keep=N` retains exactly the newest N
                    // available versions (with HEAD as the unavoidable floor
                    // when N=0).
                    let versions = ds
                        .versions()
                        .await
                        .map_err(|error| OmniError::Lance(error.to_string()))?;
                    let retain = (keep as usize).max(1);
                    let cutoff = if versions.len() <= retain {
                        versions.first()
                    } else {
                        versions.get(versions.len() - retain)
                    }
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "cleanup found no versions for open table '{table_key}'"
                        ))
                    })?;
                    Some(cutoff.version)
                } else {
                    None
                };
                let before_version = match (requested_before_version, live_main_floor) {
                    (Some(requested), Some(floor)) => Some(requested.min(floor)),
                    (None, Some(floor)) => Some(floor),
                    (requested, None) => requested,
                };
                let policy = CleanupPolicy {
                    before_timestamp,
                    before_version,
                    delete_unverified: false,
                    error_if_tagged_old_versions: false,
                    clean_referenced_branches: false,
                    delete_rate_limit: None,
                };
                lance::dataset::cleanup::cleanup_old_versions(ds, policy)
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
/// table key (e.g. `node:Person`). Per-owner failures are isolated and
/// recorded here, not propagated — the next reconcile converges.
#[derive(Debug, Clone, Default)]
pub struct BranchReconcileStats {
    pub reclaimed: Vec<(String, String)>,
    pub failures: Vec<(String, String)>,
}

/// Drop every per-table Lance branch fork the manifest does not reference.
/// Graph lineage lives in `__manifest`; the retired standalone commit datasets
/// have no branch-ref cleanup path here.
///
/// Two origins produce a manifest-unreferenced fork:
///   1. A `branch_delete` flips the manifest authority (atomic) but a
///      downstream best-effort reclaim does not complete — the whole branch is
///      gone from the manifest, but a `tree/{branch}/` ref lingers.
///   2. A first-write fork (or a merge fork) creates the branch ref before the
///      manifest publish, then the writer dies / is cancelled — the branch is
///      still a live manifest branch, but the manifest's snapshot of it does
///      not place *this table* on the branch.
///
/// The write path self-heals (2) on the next write to the table
/// (`reclaim_orphaned_fork_and_refork`); this is the guaranteed-convergence
/// backstop that also covers (1) and any table the write path never revisits.
///
/// The orphan test is therefore **per-table**, not per-branch-name: a Lance
/// branch `B` on table `T` is an orphan iff `B` is not a live manifest branch
/// at all (origin 1) OR the manifest's branch-`B` snapshot does not place `T`
/// on `B` (origin 2). A legitimately-forked table (`table_branch == Some(B)`)
/// is kept. `main` and internal/system branches are never candidates. Lance
/// refuses to force-delete a branch with referencing descendants, so children
/// are dropped before parents (longest name first). Idempotent and authority-
/// derived: no-ops once reconciled, and degrades to finding nothing if a future
/// Lance atomic multi-dataset branch op prevents orphans from forming.
#[cfg(all(test, feature = "failpoints"))]
pub async fn reconcile_orphaned_branches(db: &Omnigraph) -> Result<BranchReconcileStats> {
    let catalog = db.catalog();
    reconcile_orphaned_branches_with_catalog(db, &catalog).await
}

async fn reconcile_orphaned_branches_with_catalog(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
) -> Result<BranchReconcileStats> {
    use std::collections::{HashMap, HashSet};

    // Live manifest branches: the set whose per-table placements are
    // authoritative. A branch absent here is a whole-branch (origin-1) orphan.
    let live_branches: HashSet<String> = db
        .coordinator
        .read()
        .await
        .all_branches()
        .await?
        .into_iter()
        .collect();

    let resolved = db.resolved_branch_target(None).await?;
    let snapshot = resolved.snapshot;
    let table_targets: Vec<(String, String)> = all_table_keys(catalog)
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.entry(&table_key)?;
            let full_path = format!("{}/{}", db.root_uri, entry.table_path);
            Some((table_key, full_path))
        })
        .collect();

    let mut stats = BranchReconcileStats::default();
    // Per-branch snapshots are resolved once and cached across tables (few
    // branches in practice); origin-2 detection consults the branch's own view.
    // Failures are cached too: one branch-level read failure should not refetch
    // and append duplicate per-table noise for every table that lists the ref.
    let mut branch_snapshots: HashMap<String, crate::db::Snapshot> = HashMap::new();
    let mut failed_branch_snapshots: HashSet<String> = HashSet::new();

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

        // Decide per (table, branch) whether the fork is an orphan.
        let mut orphans: Vec<String> = Vec::new();
        for branch in listed {
            // `main` is not a named Lance branch; system/internal branches
            // (e.g. the schema-apply lock) own legitimate forks — never touch.
            if branch == "main" || crate::db::is_internal_system_branch(&branch) {
                continue;
            }
            let is_orphan = if !live_branches.contains(&branch) {
                true // origin 1: whole branch gone from the manifest
            } else {
                // origin 2: live branch, but does the manifest place THIS
                // table on it? Resolve (and cache) the branch's snapshot.
                if failed_branch_snapshots.contains(&branch) {
                    continue;
                }
                if !branch_snapshots.contains_key(&branch) {
                    let branch_snapshot = match crate::failpoints::maybe_fail(
                        crate::failpoints::names::CLEANUP_RESOLVE_BRANCH_SNAPSHOT,
                    ) {
                        Ok(()) => db.snapshot_for_branch(Some(&branch)).await,
                        Err(injected) => Err(injected),
                    };
                    match branch_snapshot {
                        Ok(snap) => {
                            branch_snapshots.insert(branch.clone(), snap);
                        }
                        Err(err) => {
                            tracing::warn!(
                                target: "omnigraph::cleanup",
                                table = %table_key,
                                branch = %branch,
                                error = %err,
                                "resolving branch snapshot failed during reconcile; skipping",
                            );
                            stats.failures.push((table_key.clone(), err.to_string()));
                            failed_branch_snapshots.insert(branch.clone());
                            continue;
                        }
                    }
                }
                branch_snapshots[&branch]
                    .entry(&table_key)
                    .map(|e| e.table_branch.as_deref() != Some(branch.as_str()))
                    .unwrap_or(true)
            };
            if is_orphan {
                orphans.push(branch);
            }
        }
        // Children before parents (longest name first) so Lance's referenced-
        // parent RefConflict cannot block reclamation.
        orphans.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));

        for branch in orphans {
            // Serialize against in-process live writers before destroying a ref.
            // A first-write fork holds the per-(table, branch) write queue from
            // before the fork through the manifest publish; on a LIVE branch its
            // in-flight fork looks exactly like an origin-2 orphan (manifest not
            // yet advanced). Acquire the same queue so cleanup waits for any such
            // writer, then RE-VALIDATE under the queue with a fresh read: if the
            // writer published in the meantime (table now placed on the branch),
            // it is no longer an orphan — skip it. (Cross-process writers remain
            // the documented one-winner-CAS gap.) One key held at a time → no
            // lock-order inversion against multi-table `acquire_many` writers.
            let _guard = db
                .write_queue()
                .acquire(&(table_key.clone(), Some(branch.clone())))
                .await;
            // Decide under the queue from FRESH authority via the shared
            // classifier (same decision the write-path reclaim uses) — never
            // from the sweep-start `live_branches` capture. A branch created
            // AFTER that capture is absent from the stale set yet may already
            // carry a legitimately-published fork (an in-process writer held
            // this queue through its fork+publish; we just waited on it), so a
            // stale "origin-1 ⇒ delete" shortcut would destroy a live fork.
            // Only `Orphan` is reclaimed; `Indeterminate` (transient read) is
            // skipped and recorded. (Cross-process writers remain the documented
            // one-winner-CAS gap.) One key held at a time → no lock-order
            // inversion vs multi-table `acquire_many` writers.
            match super::table_ops::classify_fork_ref(db, &table_key, &branch, None).await {
                super::table_ops::ForkRefStatus::Orphan => {}
                super::table_ops::ForkRefStatus::Legitimate => continue,
                super::table_ops::ForkRefStatus::Indeterminate => {
                    tracing::warn!(
                        target: "omnigraph::cleanup",
                        table = %table_key,
                        branch = %branch,
                        "fresh re-check inconclusive during reconcile; skipping to avoid \
                         destroying a possibly-live fork (will retry next cleanup)",
                    );
                    stats.failures.push((
                        table_key.clone(),
                        format!("indeterminate fork status for {branch}"),
                    ));
                    continue;
                }
            }
            let outcome = match crate::failpoints::maybe_fail(
                crate::failpoints::names::CLEANUP_RECONCILE_FORK,
            ) {
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

    Ok(stats)
}

pub(super) fn all_table_keys(catalog: &omnigraph_compiler::catalog::Catalog) -> Vec<String> {
    let mut keys: Vec<String> = catalog
        .node_types
        .keys()
        .map(|n| format!("node:{}", n))
        .chain(catalog.edge_types.keys().map(|n| format!("edge:{}", n)))
        .collect();
    keys.sort();
    keys
}

#[cfg(all(test, feature = "failpoints"))]
mod tests {
    use super::*;
    use crate::failpoints::ScopedFailPoint;
    use crate::loader::{LoadMode, load_jsonl};

    /// The internal-table compaction retry classifier: a concurrent live writer
    /// preempting our `Rewrite` is retryable (Lance prescribes app-rerun, and
    /// compact_files does not auto-retry it); a non-conflict error is not (must not
    /// be masked by a blind retry).
    #[test]
    fn retryable_lance_conflicts_are_classified() {
        assert!(is_retryable_lance_conflict(
            &lance::Error::retryable_commit_conflict_source(
                1,
                Box::new(std::io::Error::other("preempted by concurrent write")),
            )
        ));
        assert!(is_retryable_lance_conflict(
            &lance::Error::too_much_write_contention("contended")
        ));
        assert!(!is_retryable_lance_conflict(&lance::Error::invalid_input(
            "not a conflict"
        )));
    }

    fn node_table_uri(root: &str, type_name: &str) -> String {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in type_name.as_bytes() {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100_0000_01b3);
        }
        format!("{}/nodes/{hash:016x}", root.trim_end_matches('/'))
    }

    #[tokio::test]
    async fn reconcile_caches_live_branch_snapshot_resolution_failure() {
        let _scenario = fail::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let schema = "node Person { name: String @key }\nnode Company { name: String @key }\n";
        let mut db = Omnigraph::init(uri, schema).await.unwrap();
        load_jsonl(
            &mut db,
            "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\"}}\n\
             {\"type\":\"Company\",\"data\":{\"name\":\"Acme\"}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();
        db.branch_create("feature").await.unwrap();

        for type_name in ["Person", "Company"] {
            let table_uri = node_table_uri(uri, type_name);
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&table_uri).await.unwrap();
            let base = ds.version().version;
            ds.create_branch("feature", base, None).await.unwrap();
        }

        let _fp = ScopedFailPoint::new(
            crate::failpoints::names::CLEANUP_RESOLVE_BRANCH_SNAPSHOT,
            "return",
        );
        let stats = reconcile_orphaned_branches(&db).await.unwrap();

        assert_eq!(
            stats.failures.len(),
            1,
            "one live-branch snapshot resolution failure should be reported once, \
             not once per table: {:?}",
            stats.failures
        );
        assert!(
            stats.failures[0]
                .1
                .contains("cleanup.resolve_branch_snapshot"),
            "the recorded failure should be the branch-snapshot resolution failure: {:?}",
            stats.failures
        );
        assert!(
            stats.reclaimed.is_empty(),
            "unreadable live-branch refs must be left for the next cleanup run"
        );
    }
}
