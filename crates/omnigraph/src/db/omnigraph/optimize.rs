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
//!   version to the `__manifest`** so the manifest's `table_version` tracks the
//!   compacted Lance HEAD (reads pin the manifest version, so without the
//!   publish compaction would be invisible to readers and would break the
//!   HEAD-vs-manifest precondition of schema apply / strict writes). Compaction
//!   is content-preserving (Lance `Operation::Rewrite` "reorganizes data
//!   without semantic modification"), so old fragments remain reachable via
//!   older manifest versions until `cleanup` runs.
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
/// reason rather than sniffing a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// The table has one or more `Blob` columns. Lance `compact_files` forces
    /// `BlobHandling::AllBinary`, which mis-decodes blob-v2 columns; see
    /// [`LANCE_SUPPORTS_BLOB_COMPACTION`] and `docs/dev/lance.md`.
    BlobColumnsUnsupportedByLance,
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
            SkipReason::BlobColumnsUnsupportedByLance => "blob_columns_unsupported_by_lance",
            SkipReason::DriftNeedsRepair => "drift_needs_repair",
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
    /// normal compaction/no-op/blob skips.
    pub manifest_version: Option<u64>,
    /// Lance HEAD version observed by optimize for drift skips. `None` for
    /// normal compaction/no-op/blob skips.
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

    /// Stat for a table that was deliberately skipped (compaction not attempted).
    fn skipped(table_key: String, reason: SkipReason) -> Self {
        Self {
            table_key,
            fragments_removed: 0,
            fragments_added: 0,
            committed: false,
            skipped: Some(reason),
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

/// Run Lance `compact_files` on every node + edge table on `main`, publishing
/// each compacted table's new version to the `__manifest`. Tables run in
/// parallel (bounded concurrency); each is fault-isolated only at the Lance
/// level — a publish error is propagated (the recovery sidecar covers it).
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

    let snapshot = db.fresh_snapshot_for_branch(None).await?;

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

    // NB: do NOT early-return when `table_tasks` is empty (a schema with no
    // node/edge types) — the internal system tables below must still be compacted.
    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);

    let stats: Vec<Result<TableOptimizeStats>> = futures::stream::iter(table_tasks.into_iter())
        .map(move |(table_key, full_path, has_blob)| async move {
            optimize_one_table(db, table_key, full_path, has_blob).await
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Invalidate caches for any table that published a compaction — done BEFORE
    // propagating a sibling table's error, since the published versions are
    // durable and reads must observe the new fragment layout (Lance invalidates
    // the original row addresses on rewrite). The CSR/CSC graph topology index
    // is rebuilt only when an edge table moved. Mirrors schema_apply's
    // post-publish invalidation.
    let any_committed = stats.iter().any(|s| matches!(s, Ok(st) if st.committed));
    let edge_committed = stats
        .iter()
        .any(|s| matches!(s, Ok(st) if st.committed && st.table_key.starts_with("edge:")));
    if any_committed {
        db.runtime_cache.invalidate_all().await;
        if edge_committed {
            db.invalidate_graph_index().await;
        }
    }

    // Compact the internal system tables too (RFC-013 step 2). They are not
    // catalog-tracked, so they take a separate, simpler path (`compact_internal_table`):
    // compact in place, no manifest publish, no sidecar. Appended after the
    // data-table stats so the data-table cache invalidation above is computed from
    // data-table stats only; each internal compaction does its own coordinator
    // refresh for cache coherence.
    let mut all = stats;
    // One source of truth for the internal system tables optimize compacts. The
    // commit graph is THREE tables, not one: the DAG (`_graph_commits`), the actor
    // map (`_graph_commit_actors`, appended by every *authenticated* write — the
    // production server/CLI path always carries an actor), and the manifest. Missing
    // any leaves an O(history) scan on a live write path. `__manifest` is always
    // present (created at init); the two commit-graph tables may be absent (the
    // coordinator opens them as `Option`, gated on existence — graphs predating the
    // commit graph, and the actor table is itself optional), so guard each with the
    // same existence check rather than letting `Dataset::open` error and fail the
    // whole optimize.
    let root = db.root_uri();
    let internal_tables: [(&str, String); 3] = [
        ("__manifest", crate::db::manifest::manifest_uri(root)),
        (
            "_graph_commits",
            crate::db::commit_graph::graph_commits_uri(root),
        ),
        (
            "_graph_commit_actors",
            crate::db::commit_graph::graph_commit_actors_uri(root),
        ),
    ];
    for (table_key, uri) in internal_tables {
        if table_key == "__manifest" || db.storage_adapter().exists(&uri).await? {
            all.push(compact_internal_table(db, table_key, uri).await);
        }
    }

    all.into_iter().collect()
}

/// Compact one table and publish the compacted version to the `__manifest`.
///
/// Compaction (`compact_files`) advances the *dataset's* Lance HEAD via a
/// reserve-fragments + rewrite commit, but Lance knows nothing about the
/// `__manifest`. To keep the manifest the single authority for each table's
/// visible version (invariant 2), optimize must publish the compacted version.
/// The Lance-HEAD-before-manifest-publish gap is unavoidable (Lance has no
/// staged/uncommitted compaction), so it is covered by a recovery sidecar like
/// the other multi-commit writers; roll-forward is always safe because
/// compaction is content-preserving.
async fn optimize_one_table(
    db: &Omnigraph,
    table_key: String,
    full_path: String,
    has_blob: bool,
) -> Result<TableOptimizeStats> {
    // Lance `compact_files` mis-decodes blob-v2 columns under the forced
    // `BlobHandling::AllBinary` read (see LANCE_SUPPORTS_BLOB_COMPACTION). Skip
    // blob-bearing tables before acquiring the write queue; `repair` is the
    // operator tool for full manifest/head drift classification.
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

    // Serialize the whole compact→publish against concurrent mutations on this
    // (table, main): compaction is a Rewrite op that retryable-conflicts with a
    // concurrent Merge/Update/Delete on overlapping fragments, and an
    // interleaved write would also move the manifest version out from under the
    // CAS below. Holding the queue makes the CAS baseline read under it exact.
    let _guard = db
        .write_queue()
        .acquire_many(&[(table_key.clone(), None)])
        .await;

    // `compact_files` is a Lance-only maintenance API that needs `&mut Dataset`.
    // The `TableStorage` trait deliberately does not surface it (the staged-write
    // invariant covers writes; compaction is a separate concern). Unwrap the
    // opaque `SnapshotHandle` via `into_dataset()` (`pub(crate)`, gated to the
    // maintenance path).
    let handle = db
        .storage()
        .open_dataset_head_for_write(&table_key, &full_path, None)
        .await?;
    let mut ds = handle.into_dataset();

    // CAS baseline: the table's current manifest version, read under the queue
    // (in-memory coordinator snapshot, no storage I/O — stable for this section).
    let expected_version = db
        .fresh_snapshot_for_branch(None)
        .await?
        .entry(&table_key)
        .map(|e| e.table_version)
        .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;

    let lance_head_version = ds.version().version;
    if lance_head_version < expected_version {
        return Err(OmniError::manifest_internal(format!(
            "table '{}' Lance HEAD version {} is behind manifest version {}",
            table_key, lance_head_version, expected_version
        )));
    }
    if lance_head_version > expected_version {
        tracing::warn!(
            target: "omnigraph::optimize",
            table = %table_key,
            manifest_version = expected_version,
            lance_head_version,
            "skipping compaction: Lance HEAD is ahead of the manifest; run `omnigraph repair` \
             to classify and publish covered maintenance drift explicitly",
        );
        return Ok(TableOptimizeStats::skipped_for_drift(
            table_key,
            expected_version,
            lance_head_version,
        ));
    }

    // Precise "will it compact?" check — `plan_compaction` also accounts for
    // deletion materialization (which can rewrite even a single fragment).
    let options = CompactionOptions::default();
    let plan = plan_compaction(&ds, &options)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let will_compact = plan.num_tasks() > 0;
    // Even when there is nothing to compact, the table may still have index
    // work: rows appended since the index was built (e.g. via `ingest --mode
    // merge`) are scanned unindexed until folded in (needs_reindex), OR a
    // declared `@index` was never built — schema apply records the intent but
    // defers the physical build (iss-848), so optimize is the operator-facing
    // reconciler that materializes it (needs_index_create). Any of the three is
    // enough to enter the publish path. If NONE, this table is a no-op and must
    // NOT be pinned in a sidecar — a zero-commit pin classifies NoMovement on
    // recovery and forces an all-or-nothing rollback of sibling tables'
    // legitimate work. Uncovered pre-existing manifest/HEAD drift is skipped
    // above and goes through explicit repair, so this only runs on a healthy
    // table under the per-table queue + sidecar.
    let needs_reindex = TableStore::has_unindexed_fragments(&ds).await?;
    // needs_index_work_* checks "a declared index is missing AND row_count > 0",
    // so empty tables stay no-ops (never pinned). It re-reads the head under the
    // queue we already hold, so it is consistent with `ds`.
    let needs_index_create = if let Some(type_name) = table_key.strip_prefix("node:") {
        super::table_ops::needs_index_work_node(db, type_name, &table_key, &full_path, None).await?
    } else {
        super::table_ops::needs_index_work_edge(db, &table_key, &full_path, None).await?
    };
    if !will_compact && !needs_reindex && !needs_index_create {
        return Ok(TableOptimizeStats::compacted(
            table_key,
            &CompactionMetrics::default(),
            false,
        ));
    }

    // Phase A: recovery sidecar BEFORE any HEAD-advancing op (compaction or
    // index optimize), so a crash before the manifest publish rolls forward on
    // next open.
    let sidecar = crate::db::manifest::new_sidecar(
        crate::db::manifest::SidecarKind::Optimize,
        None,
        // optimize is system-attributed (no `optimize_as` actor API today).
        None,
        vec![crate::db::manifest::SidecarTablePin {
            table_key: table_key.clone(),
            table_path: full_path.clone(),
            expected_version,
            // Lower bound — compaction commits N≥1 versions (reserve + rewrite);
            // the classifier loose-matches SidecarKind::Optimize.
            post_commit_pin: expected_version + 1,
            // Optimize uses the loose match (drift is derived state), not
            // BranchMerge's Phase-B confirmation — left None.
            confirmed_version: None,
            table_branch: None,
        }],
    );
    let handle =
        crate::db::manifest::write_sidecar(db.root_uri(), db.storage_adapter(), &sidecar).await?;

    // Phase B: compaction (if any) then incremental index optimize — both
    // advance Lance HEAD inside the sidecar window. `compact_files` rewrites
    // fragments and drops them from existing index segments' coverage;
    // `optimize_indices` folds the rewritten and any previously-unindexed
    // fragments back in (Lance's incremental merge, not a full retrain). This
    // is the same compact -> optimize_indices sequencing LanceDB's `optimize()`
    // uses. `optimize_indices` is an inline-commit residual: lance-6.0.1
    // exposes no uncommitted variant, so like `compact_files` it commits
    // directly and relies on the sidecar for recovery.
    let version_before = ds.version().version;
    let metrics: CompactionMetrics = if will_compact {
        compact_files(&mut ds, options, None)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
    } else {
        CompactionMetrics::default()
    };
    ds.optimize_indices(&OptimizeOptions::default())
        .await
        .map_err(|e| OmniError::Lance(format!("optimize_indices on {}: {}", table_key, e)))?;

    // Materialize any declared-but-missing index over the just-compacted layout,
    // reusing the build chokepoint (idempotent: skips existing indexes; fault-
    // isolates an untrainable vector column into `pending` rather than failing).
    // Run it UNCONDITIONALLY now that we are past the no-op gate — not only when
    // `needs_index_create`. A table can enter this path for compaction or
    // reindex while its sole missing index is an untrainable Vector column
    // (which `needs_index_work_*` does not count as buildable work); calling the
    // build here is what surfaces that column in `pending_indexes`, so optimize
    // can't compact a table yet silently drop the deferred-index signal.
    // Idempotent + cheap when there is nothing to build. Vector index creation
    // is an inline-commit residual; the Optimize sidecar's loose post_commit_pin
    // covers the extra commits.
    let catalog = db.catalog();
    let mut snapshot = crate::storage_layer::SnapshotHandle::new(ds);
    let pending_indexes: Vec<super::PendingIndex> =
        super::table_ops::build_indices_on_dataset_for_catalog(
            db,
            &catalog,
            &table_key,
            &mut snapshot,
        )
        .await?;
    let version_after = snapshot.dataset().version().version;
    let committed = version_after != version_before;

    // Pin the per-writer Phase B → Phase C residual for optimize: Lance HEAD has
    // advanced but the manifest publish below hasn't run.
    crate::failpoints::maybe_fail("optimize.post_phase_b_pre_manifest_commit")?;

    // Phase C: publish the compacted version to the manifest (one CAS commit,
    // expected = the version observed under the queue). On failure the sidecar
    // is intentionally left for the open-time recovery sweep to roll forward.
    if committed {
        let state = db.storage().table_state(&full_path, &snapshot).await?;
        let update = crate::db::SubTableUpdate {
            table_key: table_key.clone(),
            table_version: state.version,
            table_branch: None,
            row_count: state.row_count,
            version_metadata: state.version_metadata,
        };
        let mut expected = std::collections::HashMap::new();
        expected.insert(table_key.clone(), expected_version);
        db.coordinator
            .write()
            .await
            .commit_updates_with_actor_with_expected(&[update], &expected, None)
            .await?;
    }

    // Phase D: delete the sidecar (best-effort; recovery resolves a leftover).
    if let Err(err) = crate::db::manifest::delete_sidecar(&handle, db.storage_adapter()).await {
        tracing::warn!(
            error = %err,
            operation_id = handle.operation_id.as_str(),
            "optimize recovery sidecar cleanup failed; next open's recovery sweep will resolve it"
        );
    }

    let mut stat = TableOptimizeStats::compacted(table_key, &metrics, committed);
    stat.pending_indexes = pending_indexes;
    Ok(stat)
}

/// Bound on the app-level retry of an internal-table compaction against a
/// concurrent live writer (see [`is_retryable_lance_conflict`]).
const INTERNAL_COMPACTION_RETRY_BUDGET: u32 = 5;

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

/// Remove any stored `lance.auto_cleanup.*` config from an internal table so
/// compaction stays **non-destructive by construction**.
///
/// `compact_files` commits with a default `CommitConfig` (`skip_auto_cleanup =
/// false`) and `CompactionOptions` exposes no override, so on a dataset whose
/// stored config has `lance.auto_cleanup.interval` set, the compaction commit
/// would fire Lance's auto-cleanup hook (version GC). For the internal tables that
/// is deletion of old `__manifest` / `_graph_commits` / `_graph_commit_actors`
/// versions — the cleanup-resurrection surface (RFC-013 step 2b / Q8) and the
/// time-travel history this compaction-only step must preserve. New graphs create
/// these tables with `auto_cleanup: None` (`manifest/graph.rs`, `commit_graph.rs`)
/// so there is nothing to clear; only pre-`auto_cleanup`-fix *upgraded* graphs
/// carry the config. OmniGraph owns version cleanup explicitly (`cleanup`), so
/// Lance's hook is unwanted regardless — clearing it both makes `optimize`
/// non-destructive and aligns the table with the new-graph posture. The
/// `delete_config_keys` commit itself does not GC: the resulting manifest no longer
/// has the `interval` key, so the post-commit hook is a no-op. Returns whether any
/// config was cleared. No recovery sidecar: a config update is a single atomic Lance
/// commit with no HEAD-before-manifest-publish gap.
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
    let refs: Vec<&str> = keys.iter().map(String::as_str).collect();
    ds.delete_config_keys(&refs).await?;
    Ok(true)
}

/// Compact one INTERNAL system table (`__manifest` / `_graph_commits` /
/// `_graph_commit_actors`) in place.
///
/// Unlike catalog data tables, the internal tables are not tracked in the
/// `__manifest` (they ARE the manifest / the lineage DAG): readers open them at
/// their latest Lance HEAD, so compaction just advances that HEAD and the next
/// reader transparently observes the compacted version. That makes this path much
/// simpler than [`optimize_one_table`] — no manifest publish (nothing to publish
/// to), and no recovery sidecar (a single atomic Lance `compact_files` commit, with
/// no HEAD-before-publish gap to recover). Internal tables carry no Lance index
/// (only `object_id`'s unenforced-PK schema metadata), so no `optimize_indices`.
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
    for attempt in 0..INTERNAL_COMPACTION_RETRY_BUDGET {
        let handle = db
            .storage()
            .open_dataset_head_for_write(table_key, &uri, None)
            .await?;
        let mut ds = handle.into_dataset();

        // Keep optimize non-destructive by construction (see clear_stale_auto_cleanup_config).
        // Returns whether it committed a config-strip (which advances Lance HEAD).
        let cleared_config = match clear_stale_auto_cleanup_config(&mut ds).await {
            Ok(cleared) => cleared,
            Err(e) => {
                if attempt + 1 < INTERNAL_COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e)
                {
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
            Err(e)
                if attempt + 1 < INTERNAL_COMPACTION_RETRY_BUDGET
                    && is_retryable_lance_conflict(&e) =>
            {
                continue;
            }
            Err(e) => return Err(OmniError::Lance(e.to_string())),
        }
    }
    Err(OmniError::manifest_conflict(format!(
        "internal-table compaction of {table_key} exhausted {INTERNAL_COMPACTION_RETRY_BUDGET} \
         retries against concurrent writers"
    )))
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

/// Drop every per-table and commit-graph Lance branch fork the manifest does
/// not reference.
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
pub async fn reconcile_orphaned_branches(db: &Omnigraph) -> Result<BranchReconcileStats> {
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
    let table_targets: Vec<(String, String)> = all_table_keys(&db.catalog())
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
                    let branch_snapshot =
                        match crate::failpoints::maybe_fail("cleanup.resolve_branch_snapshot") {
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
            match super::table_ops::classify_fork_ref(db, &table_key, &branch).await {
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

    // Commit-graph orphans are whole-branch (not per-table), so the simple
    // "branch name not in the live set" test still applies there.
    if let Err(err) = reconcile_commit_graph_orphans(db, &live_branches, &mut stats).await {
        tracing::warn!(
            target: "omnigraph::cleanup",
            error = %err,
            "commit-graph orphan reconcile failed; will retry next cleanup",
        );
        stats
            .failures
            .push(("_graph_commits".to_string(), err.to_string()));
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
                stats
                    .failures
                    .push(("_graph_commits".to_string(), err.to_string()));
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

        let _fp = ScopedFailPoint::new("cleanup.resolve_branch_snapshot", "return");
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
