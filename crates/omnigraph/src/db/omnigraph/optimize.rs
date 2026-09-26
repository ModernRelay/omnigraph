//! Lance compaction + version cleanup exposed at the graph level.
//!
//! Lance accumulates many small `.lance` fragment files per backing dataset over the
//! life of a graph: each `write`, `load`, and `change` op appends one or more
//! fragments and a new manifest. Over long timescales this hurts open times
//! and S3 object counts without improving anything.
//!
//! Two dials:
//!
//! * `optimize_all_datasets` — Lance `compact_files` on every dataset. Rewrites
//!   small fragments into fewer large ones, then **publishes the compacted
//!   versions together in one `__manifest` batch** so each persisted
//!   `table_version` column tracks the compacted Lance HEAD (reads pin the
//!   published dataset version, so without the publish compaction would be invisible to readers and would break the
//!   HEAD-vs-manifest precondition of schema apply / strict writes). Compaction
//!   is content-preserving (Lance `Operation::Rewrite` "reorganizes data
//!   without semantic modification"), so old fragments remain reachable via
//!   older dataset versions until `cleanup` runs.
//! * `cleanup_all_datasets` — Lance `cleanup_old_versions` on every dataset.
//!   Removes manifests (and their unique fragments) older than the configured
//!   retention, capped at the oldest main-dataset version inherited by any live
//!   lazy graph branch. Destructive to unreferenced version history — callers
//!   should gate this behind an explicit confirm flag at the CLI layer.
//!
//! Both orchestrate the graph's node + edge datasets from main authority;
//! cleanup preserves both Lance-referenced native branch history and the
//! graph-level lazy-branch references Lance cannot observe.

use std::time::Duration;

use futures::stream::StreamExt;
use lance::dataset::optimize::{
    CompactionMetrics, CompactionOptions, compact_files, plan_compaction,
};
use lance::index::{DatasetIndexExt, DatasetIndexInternalExt};

use super::*;
use crate::seams::{decide_seam, fail};

/// How many datasets to optimize/cleanup concurrently. Each has separate
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

/// Retention knobs for [`cleanup_all_datasets`]. At least one must be set or
/// nothing is cleaned. If both are set, Lance applies them as AND (a manifest
/// is kept if it satisfies either — i.e. only manifests older than BOTH the
/// time cutoff AND the version cutoff are removed).
#[derive(Debug, Clone, Default)]
pub struct CleanupPolicyOptions {
    /// Keep this many most-recent versions when pruning retained datasets.
    /// This count does not retain wholly unused forks or count graph commits.
    pub keep_versions: Option<u32>,
    /// Only remove versions and unused fork objects older than this duration.
    pub older_than: Option<Duration>,
}

/// Why `optimize` did not compact a dataset. Typed so callers branch on the
/// reason rather than sniffing a string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum SkipReason {
    /// The Lance dataset HEAD is ahead of the version recorded in
    /// `__manifest`, and no pending pin explains that movement. `optimize`
    /// cannot infer whether the drift is benign maintenance or an external
    /// semantic write, so it leaves the dataset untouched and points operators at
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
            SkipReason::DriftNeedsRepair => {
                "published dataset/Lance HEAD drift — run omnigraph repair"
            }
        };
        f.write_str(msg)
    }
}

/// Per-dataset outcome of `optimize_all_datasets`. This is a returned result type,
/// not built by callers, so it is `#[non_exhaustive]`: future fields stay
/// non-breaking and downstream code reads fields rather than constructing it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DatasetOptimizeStats {
    /// Qualified graph type key, or `__manifest` for the system dataset.
    pub type_key: String,
    /// Number of source fragments that were rewritten by Lance.
    pub fragments_removed: usize,
    /// Number of new, larger fragments Lance produced.
    pub fragments_added: usize,
    /// Whether this dataset advanced to a new Lance version that this run
    /// published through the graph manifest. This may result from compaction,
    /// index maintenance or materialization, or stale `auto_cleanup` removal.
    pub committed: bool,
    /// `Some(reason)` if this dataset was deliberately not compacted. When set,
    /// `fragments_removed == 0`, `fragments_added == 0`, and `!committed`.
    pub skipped: Option<SkipReason>,
    /// Published dataset version observed by optimize for drift skips. `None` for
    /// normal compaction/no-op outcomes.
    pub published_dataset_version: Option<u64>,
    /// Lance HEAD version observed by optimize for drift skips. `None` for
    /// normal compaction/no-op outcomes.
    pub lance_head_version: Option<u64>,
    /// Index work deferred this run, with the reason and remedy: a vector
    /// property without trainable vectors, or full-text coverage requiring an
    /// explicit rebuild. Deferred work alone does not stage or publish.
    pub pending_indexes: Vec<super::PendingIndex>,
}

impl DatasetOptimizeStats {
    /// Stat for a dataset that Lance actually compacted.
    fn compacted(type_key: String, metrics: &CompactionMetrics, committed: bool) -> Self {
        Self {
            type_key,
            fragments_removed: metrics.fragments_removed,
            fragments_added: metrics.fragments_added,
            committed,
            skipped: None,
            published_dataset_version: None,
            lance_head_version: None,
            pending_indexes: Vec::new(),
        }
    }
}

/// Per-dataset outcome of `cleanup_all_datasets`. `error` is `Some` when this
/// dataset's version GC failed; cleanup is fault-isolated per dataset, so a
/// single dataset's failure is recorded here rather than aborting the whole sweep.
#[derive(Debug, Clone)]
pub struct DatasetCleanupStats {
    /// Qualified graph type key, or `__manifest` for the system dataset.
    pub type_key: String,
    pub bytes_removed: u64,
    pub old_versions_removed: u64,
    pub error: Option<String>,
    /// Detached manifests the tracing collector removes: published once and
    /// named by no retained `__manifest` version.
    pub manifests_removed: u64,
    /// Detached manifests no `__manifest` version ever named: staging in
    /// flight or abandoned.
    pub unpublished_manifests: u64,
    pub unpublished_bytes: u64,
    /// Linear versions above the newest pin any registration names.
    pub foreign_versions: Vec<u64>,
}

struct OptimizeTableTask {
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    full_path: String,
    expected_version: u64,
    entry: crate::db::manifest::DatasetEntry,
    witness: crate::table_store::StagingWitness,
}

struct PreparedOptimizeTable {
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    full_path: String,
    expected_version: u64,
    initial_snapshot: crate::storage_layer::SnapshotHandle,
    witness: crate::table_store::StagingWitness,
    last_linear_version: Option<u64>,
}

enum OptimizePreparation {
    Work(PreparedOptimizeTable),
    Stat(DatasetOptimizeStats),
}

struct OptimizeEffectOutcome {
    stat: DatasetOptimizeStats,
    effect: Option<OptimizeTableEffect>,
}

/// One table's staged maintenance: the pin update to publish and the version it
/// was planned from.
struct OptimizeTableEffect {
    update: crate::db::DatasetUpdate,
    expected_version: u64,
}

decide_seam! {
    pub static OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT = ("optimize.post_phase_b_pre_manifest_commit", Unreachable, [Fail]);
}

decide_seam! {
    /// After Optimize captures its authority token, before the schema -> main
    /// -> table gates and the revalidation that consumes it. Tests advance the
    /// graph in this window and prove Optimize refuses rather than planning
    /// against authority that has already moved.
    pub static OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES = ("optimize.post_authority_capture_pre_gates", Unreachable, [Fail]);
}

/// Run Lance maintenance across every node + edge dataset on `main` under one
/// graph visibility envelope. Physical dataset work remains bounded-parallel,
/// but every productive dataset stages detached and the batch publishes once
/// with an exact pin CAS, so one public Optimize produces at most one graph
/// commit. The final physical `__manifest` compaction remains outside that
/// graph-visible envelope because the system dataset is read directly at HEAD.
pub async fn optimize_all_datasets(db: &Omnigraph) -> Result<Vec<DatasetOptimizeStats>> {
    let _export_exclusion = db.reserve_export_destructive_control()?;
    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("optimize").await?;

    // Capture complete graph authority before entering any writer gate, then
    // revalidate it after schema -> main -> table acquisition. A concurrent
    // graph or schema publish therefore refuses this attempt before any
    // physical maintenance effect.
    let authority_txn = db.open_write_txn(None).await?;
    fail(&OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES)?;

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
    // Retain main through the final physical-only __manifest compaction.
    let _main_branch_guard = db.write_queue().acquire_branch(None).await;

    let table_keys = all_table_keys(&catalog);
    let queue_keys = table_keys
        .iter()
        .map(|table_key| (table_key.clone(), None))
        .collect::<Vec<_>>();
    let table_guards = db.write_queue().acquire_many(&queue_keys).await;

    let snapshot = db.revalidate_write_txn(&authority_txn).await?;

    // Whether this run advanced any edge table — consumed by the graph-index
    // artifact gate at the tail (a no-work optimize skips the rebuild+PUT).
    let mut edge_tables_committed = false;

    let witness = authority_txn.authority.staging_witness()?;
    let table_tasks = table_keys
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.dataset(&table_key)?;
            Some(OptimizeTableTask {
                identity: entry.identity,
                table_key,
                full_path: format!("{}/{}", db.root_uri, entry.dataset_path),
                expected_version: entry.published_dataset_version,
                entry: entry.clone(),
                witness: witness.clone(),
            })
        })
        .collect::<Vec<_>>();

    // NB: do NOT early-return when `table_tasks` is empty (a schema with no
    // node/edge types) — the internal system tables below must still be compacted.
    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);

    let preparations: Vec<Result<OptimizePreparation>> = futures::stream::iter(table_tasks)
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
        let effect_concurrency = maint_concurrency().min(prepared.len()).max(1);
        let effect_results: Vec<Result<OptimizeEffectOutcome>> = futures::stream::iter(prepared)
            .map(|work| {
                let catalog = std::sync::Arc::clone(&catalog);
                async move { apply_optimize_table_effects(db, catalog.as_ref(), work).await }
            })
            .buffer_unordered(effect_concurrency)
            .collect()
            .await;
        let mut outcomes = Vec::new();
        for result in effect_results {
            outcomes.push(result?);
        }
        fail(&OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT)?;
        let mut updates = Vec::new();
        let mut expected_versions = crate::db::manifest::ExpectedTableVersions::new();
        for outcome in &mut outcomes {
            if let Some(effect) = outcome.effect.take() {
                expected_versions.insert(
                    effect.update.identity,
                    crate::db::manifest::TableVersionExpectation {
                        table_key: effect.update.type_key.clone(),
                        table_version: effect.expected_version,
                        native_ref: crate::db::manifest::NativeRefPin::Exact(None),
                    },
                );
                updates.push(effect.update);
            }
        }
        let any_committed = !updates.is_empty();
        let edge_committed = updates
            .iter()
            .any(|update| update.type_key.starts_with("edge:"));
        if any_committed {
            let lineage = db.new_lineage_intent_for_branch(None, None).await?;
            super::table_ops::commit_updates_on_branch_with_expected(
                db,
                None,
                &updates,
                &expected_versions,
                None,
                &authority_txn,
                lineage,
            )
            .await?;
        }
        edge_tables_committed = edge_committed;
        stats.extend(outcomes.into_iter().map(|outcome| outcome.stat));
        if any_committed {
            db.runtime_cache.invalidate_all().await;
            if edge_committed {
                db.invalidate_graph_index().await;
            }
        }
    }
    stats.sort_by(|left, right| left.type_key.cmp(&right.type_key));

    drop(table_guards);
    drop(schema_guard);

    // Compact the internal system tables too (RFC-013 step 2). They are not
    // catalog-tracked, so they take a separate, simpler path (`compact_internal_table`):
    // compact in place, no manifest publish. Appended after the
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

    // Persist the CSR/CSC adjacency artifact for the post-optimize snapshot,
    // so cold traversals load topology with one GET instead of scanning every
    // edge table. Optimize is the ONLY writer of this artifact (the query
    // path only loads); it is derived, regenerable data content-addressed by
    // the edge tables' physical identity, so a crash mid-write leaves an
    // object the loader rejects and rebuilds around — best-effort by design,
    // like the physical `__manifest` compaction above it, and running under
    // the still-held main branch gate so the snapshot it keys on is settled.
    if let Err(error) = persist_graph_index_artifact(db, &catalog, edge_tables_committed).await {
        tracing::warn!(
            target: "omnigraph::optimize",
            error = %error,
            "graph index artifact persist failed; traversals keep building in memory"
        );
    }

    all.into_iter().collect()
}

/// Build the full-catalog graph index from a fresh main snapshot and write it
/// as the persisted adjacency artifact. Skipped when the catalog declares no
/// edge types (nothing to traverse), when any declared edge table is not yet
/// materialized (`save` would refuse the incomplete identity set — checked
/// BEFORE the full edge scan, so a partially-materialized store pays nothing),
/// and when this run advanced no edge table AND an artifact already exists: a
/// no-work optimize must not pay a full edge scan plus a PUT. Accepted
/// tradeoffs of the existence gate, both safe (loads reject on identity
/// stamps or the format version and rebuild in memory), costing only the
/// artifact's speedup for the window: (a) edge data written since the last
/// artifact WITHOUT compaction work leaves the artifact stale until an
/// edge-committing optimize; (b) an artifact in an older FORMAT on a store
/// with no edge writes is likewise rewritten only by the next edge-committing
/// optimize — the gate cannot cheaply see inside the object (a bounded read
/// of the whole body is the only primitive; a ranged header read is the
/// planned follow-up).
async fn persist_graph_index_artifact(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
    edge_tables_committed: bool,
) -> Result<()> {
    if catalog.edge_types.is_empty() {
        return Ok(());
    }
    let uri = crate::graph_index::persist::artifact_uri(db.root_uri());
    if !edge_tables_committed && db.storage_adapter().exists(&uri).await? {
        tracing::debug!(
            target: "omnigraph::optimize",
            "graph index artifact refresh skipped: no edge table advanced and an artifact exists"
        );
        return Ok(());
    }
    let edge_types: std::collections::HashMap<String, (String, String)> = catalog
        .edge_types
        .iter()
        .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
        .collect();
    let snapshot = db.snapshot_for_branch(None).await?;
    // `save` refuses a partially-materialized store (no complete identity to
    // stamp); check that BEFORE paying the full-catalog edge scan.
    if edge_types
        .keys()
        .any(|edge| snapshot.dataset(&format!("edge:{edge}")).is_none())
    {
        tracing::debug!(
            target: "omnigraph::optimize",
            "graph index artifact skipped: not every declared edge table is materialized yet"
        );
        return Ok(());
    }
    let index =
        crate::graph_index::GraphIndex::build(&snapshot, &edge_types, db.catalog().system_columns)
            .await?;
    let written =
        crate::graph_index::persist::save(&snapshot, db.storage_adapter(), &edge_types, &index)
            .await?;
    if let Some(uri) = written {
        // A fresh artifact may serve scope keys whose earlier load attempt
        // failed; those negative verdicts are stamp-keyed, and when this
        // optimize advanced no table version the stamps (hence keys) are
        // unchanged — drop them so the new object gets loaded.
        db.runtime_cache.note_artifact_replaced().await;
        tracing::debug!(
            target: "omnigraph::optimize",
            uri = %uri,
            "graph index artifact persisted"
        );
    }
    Ok(())
}

/// Pure planning: classify drift/no-work/productive state without advancing
/// any Lance HEAD. The caller holds schema -> main -> all table gates.
async fn prepare_optimize_table(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
    task: OptimizeTableTask,
) -> Result<OptimizePreparation> {
    let snapshot = db
        .open_pinned_for_write(&task.full_path, &task.entry)
        .await?;

    let options = CompactionOptions::default();
    let will_compact = plan_compaction(snapshot.dataset(), &options)
        .await
        .map_err(OmniError::storage)?
        .num_tasks()
        > 0;
    let needs_reindex = TableStore::has_foldable_unindexed_fragments(snapshot.dataset()).await?;
    let index_work = super::table_ops::index_work_status_on_dataset_for_catalog(
        db,
        catalog,
        &task.table_key,
        &snapshot,
    )
    .await?;
    if !will_compact && !needs_reindex && !index_work.needs_commit {
        let mut stat =
            DatasetOptimizeStats::compacted(task.table_key, &CompactionMetrics::default(), false);
        stat.pending_indexes = index_work.pending;
        append_deferred_full_text_indexes(&snapshot, &stat.type_key, &mut stat.pending_indexes)
            .await?;
        return Ok(OptimizePreparation::Stat(stat));
    }

    Ok(OptimizePreparation::Work(PreparedOptimizeTable {
        identity: task.identity,
        table_key: task.table_key,
        full_path: task.full_path,
        expected_version: task.expected_version,
        initial_snapshot: snapshot,
        witness: task.witness,
        last_linear_version: task.entry.version_metadata.last_linear_version(),
    }))
}

/// Deferred full-text coverage is observable status, never a promise that
/// ordinary optimize will advance this table. Recheck after physical work too:
/// stable-ID compaction can change coverage while preserving index artifacts.
async fn append_deferred_full_text_indexes(
    snapshot: &crate::storage_layer::SnapshotHandle,
    table_key: &str,
    pending: &mut Vec<super::PendingIndex>,
) -> Result<()> {
    let ds = snapshot.dataset();
    let indices = ds.load_indices().await.map_err(OmniError::storage)?;
    let full_text: std::collections::BTreeMap<_, _> = indices
        .iter()
        .filter(|index| TableStore::is_full_text_index(index))
        .map(|index| (index.name.as_str(), index))
        .collect();
    for (name, index) in full_text {
        let coverage_unknown = indices
            .iter()
            .any(|segment| segment.name == name && segment.fragment_bitmap.is_none());
        if !coverage_unknown
            && ds
                .unindexed_fragments(name)
                .await
                .map_err(OmniError::storage)?
                .is_empty()
        {
            continue;
        }
        for field in index
            .keyed_fields()
            .iter()
            .filter_map(|id| ds.schema().field_by_id(*id))
        {
            pending.push(super::PendingIndex {
                type_key: table_key.to_string(),
                property: field.name.clone(),
                reason: format!(
                    "full-text index '{name}' has incomplete or unknown coverage; \
                     run omnigraph rebuild-full-text-indexes <URI> --branch main"
                ),
            });
        }
    }
    Ok(())
}

decide_seam! {
    /// After one table's detached rewrite or index link committed, before the
    /// next link or table (RFC 0067). Nothing is published yet.
    pub static OPTIMIZE_POST_TABLE_EFFECT = ("optimize.post_table_effect", Unreachable, [Fail]);
}

decide_seam! {
    pub static OPTIMIZE_BEFORE_COMPACT = ("optimize.before_compact", Unreachable, [Fail]);
}

/// Stage one productive table's maintenance detached from its pin, as a
/// chain of at most three links: the compaction rewrite, then a whole rebuild
/// of every foldable index whose coverage lags the rewritten layout (Lance 11
/// folds only through a linear commit, so `stage_index_fold` rebuilds instead
/// of merging), then any declared-but-unbuilt index. The rewrite comes first
/// so an index is rebuilt once, over the settled layout.
async fn apply_optimize_table_effects(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
    work: PreparedOptimizeTable,
) -> Result<OptimizeEffectOutcome> {
    let table_key = work.table_key;
    let full_path = work.full_path;
    let base = work.initial_snapshot;
    let witness = work.witness;
    fail(&OPTIMIZE_BEFORE_COMPACT)?;
    let options = CompactionOptions::default();
    let mut chain = Vec::new();
    let mut tip = base.clone();
    let mut tip_identity = None;
    let mut metrics = CompactionMetrics::default();
    if let Some((staged, compaction_metrics)) =
        db.storage().stage_compaction(&base, &options).await?
    {
        let (rewrite, identity) = db
            .storage()
            .commit_staged_detached(tip, staged, &witness)
            .await?;
        metrics = compaction_metrics;
        tip = rewrite;
        tip_identity = Some(identity);
        fail(&OPTIMIZE_POST_TABLE_EFFECT)?;
    }
    // Fold every index whose coverage lags (appended fragments, or a vector
    // index that keeps row addresses and dropped the compacted fragments)
    // as a detached rebuild chained on the rewrite.
    let (fold, skipped_folds) = db.storage().stage_index_fold(&tip).await?;
    if let Some(staged) = fold {
        if tip_identity.is_some() {
            chain.push(tip.clone());
        }
        let (folded, identity) = db
            .storage()
            .commit_staged_detached(tip, staged, &witness)
            .await?;
        tip = folded;
        tip_identity = Some(identity);
        fail(&OPTIMIZE_POST_TABLE_EFFECT)?;
    }
    let mut index_work =
        super::table_ops::plan_index_work_on_dataset_for_catalog(db, catalog, &table_key, &tip)
            .await?;
    for (column, reason) in skipped_folds {
        index_work.pending.push(super::PendingIndex {
            type_key: table_key.clone(),
            property: column,
            reason: format!(
                "vector index coverage lags and the column cannot train an index: {reason}"
            ),
        });
    }
    if !index_work.specs.is_empty() {
        let staged = db
            .storage()
            .stage_create_indices(&tip, &index_work.specs)
            .await
            .map_err(|error| {
                error.with_context(format!(
                    "stage index batch on {table_key} ({:?})",
                    index_work.specs
                ))
            })?;
        if tip_identity.is_some() {
            chain.push(tip.clone());
        }
        let (indexed, identity) = db
            .storage()
            .commit_staged_detached(tip, staged, &witness)
            .await?;
        tip = indexed;
        tip_identity = Some(identity);
        fail(&OPTIMIZE_POST_TABLE_EFFECT)?;
    }
    let Some(identity) = tip_identity else {
        let mut stat = DatasetOptimizeStats::compacted(table_key, &metrics, false);
        stat.pending_indexes = index_work.pending;
        append_deferred_full_text_indexes(&tip, &stat.type_key, &mut stat.pending_indexes).await?;
        return Ok(OptimizeEffectOutcome { stat, effect: None });
    };
    let mut stat = DatasetOptimizeStats::compacted(table_key.clone(), &metrics, true);
    stat.pending_indexes = index_work.pending;
    append_deferred_full_text_indexes(&tip, &stat.type_key, &mut stat.pending_indexes).await?;
    let state = db.storage().table_state(&full_path, &tip).await?;
    let published_dataset_version = work.expected_version + 1 + chain.len() as u64;
    let version_metadata = state
        .version_metadata
        .with_staged(state.version, identity.uuid.clone())
        .with_last_linear_version(work.last_linear_version);
    let update = crate::db::DatasetUpdate {
        identity: work.identity,
        type_key: table_key,
        published_dataset_version,
        native_dataset_branch: None,
        entity_count: state.row_count,
        version_metadata,
    };
    Ok(OptimizeEffectOutcome {
        stat,
        effect: Some(OptimizeTableEffect {
            update,
            expected_version: work.expected_version,
        }),
    })
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

/// Strip a stored `lance.auto_cleanup.*` config so an internal table's linear
/// `compact_files` commit cannot fire Lance's version GC; returns whether a
/// config commit (which advances Lance HEAD) cleared anything.
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
/// to), and no detached staging. Crash safety does NOT rest on
/// single-commit atomicity: `compact_files` can emit a `ReserveFragments` commit
/// before the final `Rewrite` (and the config strip is a separate commit before
/// both), so this advances HEAD over one or more commits. That is safe
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
) -> Result<DatasetOptimizeStats> {
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
                return Err(OmniError::storage(e));
            }
        };

        let options = CompactionOptions::default();
        let plan = plan_compaction(&ds, &options)
            .await
            .map_err(OmniError::storage)?;
        if plan.num_tasks() == 0 {
            // No compaction work, but a config-strip still advanced HEAD — refresh
            // the warm coordinator handles so they observe it deterministically
            // (same cache-coherence step the successful-compaction path takes
            // below; otherwise they stay pinned until the next version probe).
            if cleared_config {
                db.coordinator.write().await.refresh().await?;
            }
            return Ok(DatasetOptimizeStats::compacted(
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
                return Ok(DatasetOptimizeStats::compacted(
                    table_key.to_string(),
                    &metrics,
                    true,
                ));
            }
            Err(e) if attempt + 1 < COMPACTION_RETRY_BUDGET && is_retryable_lance_conflict(&e) => {
                continue;
            }
            Err(e) => return Err(OmniError::storage(e)),
        }
    }
    Err(OmniError::manifest_conflict(format!(
        "internal-table compaction of {table_key} exhausted {COMPACTION_RETRY_BUDGET} \
         retries against concurrent writers"
    )))
}

decide_seam! {
    pub static CLEANUP_TABLE_GC = ("cleanup.table_gc", Unreachable, [Fail]);
}

decide_seam! {
    /// After cleanup's entry checks, before it captures authority and
    /// acquires its schema/branch/table GC gate set: the one window where a
    /// failure aborts the whole run (per-table GC failures are isolated).
    pub static CLEANUP_PRE_GATES = ("cleanup.pre_gates", Unreachable, [Fail]);
}

/// Run Lance `cleanup_old_versions` on every node + edge dataset on `main`,
/// using [`CleanupPolicyOptions`]. The latest manifest is always preserved
/// regardless (Lance invariant), and the requested cutoff is capped at the
/// oldest main-dataset version inherited by a live lazy graph branch.
pub async fn cleanup_all_datasets(
    db: &Omnigraph,
    options: CleanupPolicyOptions,
) -> Result<Vec<DatasetCleanupStats>> {
    if options.keep_versions.is_none() && options.older_than.is_none() {
        return Err(OmniError::manifest(
            "cleanup requires at least one of keep_versions or older_than",
        ));
    }

    let _export_exclusion = db.reserve_export_destructive_control()?;
    db.ensure_schema_state_valid().await?;
    db.ensure_schema_apply_idle("cleanup").await?;
    fail(&CLEANUP_PRE_GATES)?;

    // GC must be bound to one accepted graph view. Capture before acquiring
    // writer gates, and revalidate after the complete schema/branch/table
    // envelope before deleting any version history.
    let authority_txn = db.open_write_txn(None).await?;

    let _cleanup_schema_guard = db
        .write_queue()
        .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
        .await;
    db.refresh_coordinator_only().await?;
    db.ensure_schema_apply_not_locked("cleanup").await?;
    let cleanup_catalog = db.load_accepted_catalog_with_schema_gate_held().await?;
    let snapshot = db.revalidate_write_txn(&authority_txn).await?;

    let table_tasks: Vec<_> = all_table_keys(&cleanup_catalog)
        .into_iter()
        .filter_map(|table_key| {
            let entry = snapshot.dataset(&table_key)?;
            let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
            Some((table_key, full_path))
        })
        .collect();

    let graph_branches = cleanup_graph_branches(db).await?;
    let _cleanup_branch_guards = db.write_queue().acquire_branches(&graph_branches).await;
    let gc_queue_keys = db.table_queue_keys_for_branches(&graph_branches, &cleanup_catalog);
    let _cleanup_table_guards = db.write_queue().acquire_many(&gc_queue_keys).await;

    db.revalidate_write_txn(&authority_txn).await?;

    cleanup_detached_only(db, &options, &graph_branches, &table_tasks).await
}

/// The detached-only `cleanup` (RFC "Detached-only tables"): plan the collector
/// under the gates held, reconcile orphaned forks, perform each table's plan;
/// an unfinished trace keeps its table whole and its row says why.
async fn cleanup_detached_only(
    db: &Omnigraph,
    options: &CleanupPolicyOptions,
    graph_branches: &[Option<String>],
    table_tasks: &[(String, String)],
) -> Result<Vec<DatasetCleanupStats>> {
    let before_timestamp = options
        .older_than
        .map(|duration| crate::dst_clock::now_utc() - duration);
    let native_plan = prepare_native_fork_reconciliation(db, before_timestamp).await?;
    let plan = super::collector::plan_collection(db, options, graph_branches).await?;
    let mut table_tasks = table_tasks.to_vec();
    let mut scheduled_paths = table_tasks
        .iter()
        .map(|(_, path)| path.clone())
        .collect::<std::collections::HashSet<_>>();
    for planned in &plan.tables {
        if scheduled_paths.insert(planned.full_path.clone()) {
            table_tasks.push((planned.table_key.clone(), planned.full_path.clone()));
        }
    }
    let concurrency = maint_concurrency().min(table_tasks.len()).max(1);
    let plan = &plan;
    let results: Vec<DatasetCleanupStats> = futures::stream::iter(table_tasks)
        .map(|(table_key, full_path)| async move {
            let summary = plan.row_summary(&full_path);
            let mut swept = super::collector::SweepStats::default();
            let outcome: Result<()> = async {
                fail(&CLEANUP_TABLE_GC)?;
                let locations = plan
                    .tables
                    .iter()
                    .filter(|location| location.full_path == full_path)
                    .collect::<Vec<_>>();
                let unfinished = locations
                    .iter()
                    .flat_map(|location| location.errors.iter().cloned())
                    .collect::<Vec<_>>();
                if !unfinished.is_empty() {
                    return Err(OmniError::manifest_conflict(format!(
                        "the collector's trace did not finish; nothing is deleted for this \
                         table: {}",
                        unfinished.join("; ")
                    )));
                }
                for location in locations {
                    super::collector::sweep_table(db, location, &mut swept).await?;
                }
                Ok(())
            }
            .await;
            let error = outcome.err().map(|err| {
                tracing::warn!(
                    target: "omnigraph::cleanup",
                    table = %table_key,
                    error = %err,
                    "collection failed for dataset; other datasets unaffected",
                );
                err.to_string()
            });
            DatasetCleanupStats {
                type_key: table_key,
                bytes_removed: swept.bytes_removed,
                old_versions_removed: swept.manifests_removed,
                error,
                manifests_removed: swept.manifests_removed,
                unpublished_manifests: summary.unpublished_manifests,
                unpublished_bytes: summary.unpublished_bytes,
                foreign_versions: summary.foreign_versions,
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    if !plan.expired_merge_input_tags.is_empty() {
        let manifest =
            crate::db::manifest::ManifestCoordinator::collector_branch_under_control_gates(
                db.root_uri(),
                None,
                &db.control_session(),
            )
            .await?;
        crate::db::manifest::retention::release_merge_input_tags(
            manifest.dataset(),
            &plan.expired_merge_input_tags,
        )
        .await?;
    }
    let reconciled = reconcile_frozen_native_forks(db, native_plan, plan).await?;
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

/// Collect unreferenced table forks under cleanup's complete writer gates.
#[cfg(all(test, feature = "failpoints"))]
pub async fn reconcile_orphaned_branches(db: &Omnigraph) -> Result<BranchReconcileStats> {
    let _schema = db
        .write_queue()
        .acquire(&crate::db::manifest::schema_apply_serial_queue_key())
        .await;
    let catalog = db.catalog();
    let graph_branches = cleanup_graph_branches(db).await?;
    let _branches = db.write_queue().acquire_branches(&graph_branches).await;
    let table_keys = db.table_queue_keys_for_branches(&graph_branches, &catalog);
    let _tables = db.write_queue().acquire_many(&table_keys).await;
    reconcile_orphaned_branches_under_control_gates(db, None).await
}

pub(crate) async fn cleanup_graph_branches(db: &Omnigraph) -> Result<Vec<Option<String>>> {
    let mut branches = db
        .coordinator
        .read()
        .await
        .all_branches()
        .await?
        .into_iter()
        .map(|branch| if branch == "main" { None } else { Some(branch) })
        .collect::<Vec<_>>();
    branches.push(None);
    branches.sort();
    branches.dedup();
    Ok(branches)
}

struct NativeForkInventory {
    refs: std::collections::HashMap<String, lance::dataset::refs::BranchContents>,
    trees: std::collections::BTreeSet<String>,
    tagged: std::collections::HashSet<String>,
    age_retained: std::collections::HashSet<String>,
}

impl NativeForkInventory {
    fn depends_on(&self, child: &str, ancestor: &str) -> bool {
        if child == ancestor {
            return false;
        }
        if child
            .strip_prefix(ancestor)
            .is_some_and(|suffix| suffix.starts_with('/'))
        {
            return true;
        }
        let Some(child_ref) = self.refs.get(child) else {
            return false;
        };
        child_ref.parent_branch.as_deref() == Some(ancestor)
            || self.refs.get(ancestor).is_some_and(|ancestor_ref| {
                child_ref
                    .identifier
                    .find_referenced_version(&ancestor_ref.identifier)
                    .is_some()
            })
    }

    fn retain_dependencies(&self, retained: &mut std::collections::HashSet<String>) {
        loop {
            let ancestors = self
                .trees
                .iter()
                .filter(|candidate| {
                    !retained.contains(*candidate)
                        && retained.iter().any(|root| self.depends_on(root, candidate))
                })
                .cloned()
                .collect::<Vec<_>>();
            if ancestors.is_empty() {
                break;
            }
            retained.extend(ancestors);
        }
    }
}

fn is_native_layout_directory(part: &str) -> bool {
    matches!(
        part,
        "_versions" | "_transactions" | "data" | "_deletions" | "_indices"
    )
}

fn native_tree_prefix(relative: &str) -> Result<String> {
    let segments = relative.split('/').collect::<Vec<_>>();
    segments
        .iter()
        .enumerate()
        .find_map(|(index, part)| {
            if index == 0 || index + 1 == segments.len() || !is_native_layout_directory(part) {
                return None;
            }
            let branch = segments[..index].join("/");
            lance::dataset::refs::check_valid_branch(&branch)
                .is_ok()
                .then_some(branch)
        })
        .ok_or_else(|| {
            OmniError::manifest_conflict(format!(
                "cleanup cannot identify a native fork tree for '{relative}'"
            ))
        })
}

async fn native_fork_inventory(
    dataset: &lance::Dataset,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<NativeForkInventory> {
    let mut refs = crate::branch_control::list_all_branch_contents(dataset).await?;
    let active_refs = refs
        .keys()
        .cloned()
        .collect::<std::collections::HashSet<_>>();
    for (branch, archived) in
        crate::branch_control::list_archived_manifest_branches(dataset).await?
    {
        if refs
            .get(&branch)
            .is_some_and(|active| active.identifier != archived.identifier)
        {
            return Err(OmniError::manifest_conflict(format!(
                "native ref and retirement archive disagree for '{branch}'"
            )));
        }
        refs.entry(branch).or_insert(archived);
    }
    let tagged = dataset
        .tags()
        .list()
        .await
        .map_err(OmniError::storage)?
        .into_values()
        .filter_map(|tag| tag.branch)
        .collect();
    let root = dataset
        .branch_location()
        .find_main()
        .map_err(OmniError::storage)?
        .path;
    let tree = root.clone().join("tree");
    let prefix = format!("{tree}/");
    let store = dataset
        .object_store(None)
        .await
        .map_err(OmniError::storage)?;
    let mut files = store.read_dir_all(&tree, None);
    let mut trees = refs
        .keys()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let mut age_retained = std::collections::HashSet::new();
    while let Some(file) = files.next().await {
        let file = file.map_err(OmniError::storage)?;
        let relative = file
            .location
            .as_ref()
            .strip_prefix(&prefix)
            .ok_or_else(|| {
                OmniError::manifest_conflict(format!(
                    "cleanup listed native tree object outside '{tree}'"
                ))
            })?;
        if file.location.filename() == Some(crate::branch_control::RETIRED_BRANCH_ARCHIVE) {
            let suffix = format!("/{}", crate::branch_control::RETIRED_BRANCH_ARCHIVE);
            let branch = relative.strip_suffix(&suffix).ok_or_else(|| {
                OmniError::manifest_conflict("retirement archive has no native tree")
            })?;
            if !refs.contains_key(branch) {
                return Err(OmniError::manifest_conflict(format!(
                    "retirement archive for '{branch}' appeared during tree inventory"
                )));
            }
            trees.insert(branch.to_string());
            if before_timestamp.is_some_and(|cutoff| file.last_modified >= cutoff) {
                age_retained.insert(branch.to_string());
            }
            continue;
        }
        let matching_refs = refs
            .keys()
            .filter(|branch| {
                relative
                    .strip_prefix(branch.as_str())
                    .is_some_and(|suffix| {
                        suffix.strip_prefix('/').is_some_and(|path| {
                            path.split('/')
                                .next()
                                .is_some_and(is_native_layout_directory)
                        })
                    })
            })
            .collect::<Vec<_>>();
        let recent = before_timestamp.is_some_and(|cutoff| file.last_modified >= cutoff);
        if !matching_refs.is_empty() {
            if recent {
                age_retained.extend(matching_refs.into_iter().cloned());
            }
            continue;
        }
        let branch = native_tree_prefix(relative)?;
        if recent {
            age_retained.insert(branch.clone());
        }
        trees.insert(branch);
    }
    if let Some(cutoff) = before_timestamp {
        let mut observed_refs = std::collections::HashSet::new();
        {
            let directory = root.clone().join("_refs").join("branches");
            let expected = active_refs
                .iter()
                .map(|name| {
                    (
                        lance::dataset::refs::branch_contents_path(&root, name),
                        name,
                    )
                })
                .collect::<std::collections::HashMap<_, _>>();
            let mut objects = store.read_dir_all(&directory, None);
            while let Some(object) = objects.next().await {
                let object = object.map_err(OmniError::storage)?;
                if !object.location.as_ref().ends_with(".json") {
                    continue;
                }
                let name = expected.get(&object.location).ok_or_else(|| {
                    OmniError::manifest_conflict(format!(
                        "cleanup age census found an unclassified native ref '{}'",
                        object.location
                    ))
                })?;
                observed_refs.insert((*name).clone());
                if object.last_modified >= cutoff {
                    age_retained.insert((*name).clone());
                }
            }
        }
        if let Some(missing) = active_refs
            .iter()
            .find(|name| !observed_refs.contains(*name))
        {
            return Err(OmniError::manifest_conflict(format!(
                "cleanup age census cannot locate native ref '{missing}'"
            )));
        }
    }
    Ok(NativeForkInventory {
        refs,
        trees,
        tagged,
        age_retained,
    })
}

decide_seam! {
    pub static CLASSIFY_FRESH_READ = ("classify.fresh_read", Unreachable, [Fail]);
}

decide_seam! {
    pub static CLEANUP_RESOLVE_BRANCH_SNAPSHOT = ("cleanup.resolve_branch_snapshot", Unreachable, [Fail]);
}

struct NativeForkReconciliationPlan {
    manifest: Option<NativeForkInventory>,
    tables: Vec<(String, String, String, NativeForkInventory)>,
    stats: BranchReconcileStats,
}

/// Inventory before the collector's root cut. A later publication can only
/// add roots for these candidates, never introduce a new deletion candidate.
async fn prepare_native_fork_reconciliation(
    db: &Omnigraph,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<NativeForkReconciliationPlan> {
    let resolved = db.resolved_branch_target(None).await?;
    let live_identities = resolved
        .snapshot
        .datasets()
        .map(|entry| entry.identity)
        .collect::<std::collections::HashSet<_>>();
    let mut registrations =
        crate::db::manifest::ManifestCoordinator::table_registrations_under_control_gates(
            db.root_uri(),
            &db.control_session(),
        )
        .await?;
    registrations.sort_by_key(|registration| registration.identity);
    let mut plan = NativeForkReconciliationPlan {
        manifest: None,
        tables: Vec::new(),
        stats: BranchReconcileStats::default(),
    };
    let manifest_inventory = async {
        let full_path = crate::db::manifest::manifest_uri(db.root_uri());
        let handle = db.storage().open_dataset_head(&full_path, None).await?;
        crate::branch_control::archive_retired_manifest_branches(handle.dataset()).await?;
        native_fork_inventory(handle.dataset(), before_timestamp).await
    }
    .await;
    match manifest_inventory {
        Ok(inventory) => plan.manifest = Some(inventory),
        Err(error) => plan
            .stats
            .failures
            .push(("__manifest".to_string(), error.to_string())),
    }
    for registration in registrations {
        let full_path = format!("{}/{}", db.root_uri, registration.table_path);
        let inventory = async {
            let handle = match db.storage().open_dataset_head(&full_path, None).await {
                Ok(handle) => handle,
                Err(error)
                    if !live_identities.contains(&registration.identity)
                        && error.storage_failure().is_some_and(|failure| {
                            failure.kind == omnigraph_storage::StorageFailureKind::NotFound
                        }) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            let object_base = handle.dataset().branch_location().path.to_string();
            native_fork_inventory(handle.dataset(), before_timestamp)
                .await
                .map(|inventory| Some((object_base, inventory)))
        }
        .await;
        match inventory {
            Ok(Some((object_base, inventory))) => {
                plan.tables
                    .push((registration.table_key, full_path, object_base, inventory))
            }
            Ok(None) => {}
            Err(error) => plan
                .stats
                .failures
                .push((registration.table_key, error.to_string())),
        }
    }
    Ok(plan)
}

/// Roots, unreadable locations and live staging from one validated collector
/// plan protect complete native trees, including their dependency ancestors.
fn collector_retains_native_tree(
    plan: &super::collector::CollectorReport,
    full_path: &str,
    object_base: &str,
    native: &str,
) -> bool {
    if plan.tables.iter().any(|table| !table.errors.is_empty()) {
        return true;
    }
    let mut physical = object_store::path::Path::from(object_base).join("tree");
    for segment in native.split('/') {
        physical = physical.join(segment);
    }
    if plan
        .tables
        .iter()
        .flat_map(|table| &table.borrowed_origins)
        .chain(plan.auxiliary_file_origins.iter())
        .any(|origin| {
            origin == physical.as_ref()
                || origin
                    .strip_prefix(physical.as_ref())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    {
        return true;
    }
    let tree = super::promotion::table_location(full_path, Some(native));
    plan.tables
        .iter()
        .filter(|table| table.full_path == full_path)
        .any(|table| {
            let retained = !table.roots.is_empty()
                || !table.linear_roots.is_empty()
                || !table.foreign_versions.is_empty()
                || table.unpublished.iter().any(|manifest| {
                    !matches!(manifest.verdict, super::collector::StagingVerdict::Dead(_))
                });
            retained
                && (table.location == tree
                    || table
                        .location
                        .strip_prefix(&tree)
                        .is_some_and(|suffix| suffix.starts_with('/')))
        })
}

fn native_table_retention_roots(
    inventory: &NativeForkInventory,
    plan: &super::collector::CollectorReport,
    full_path: &str,
    object_base: &str,
) -> std::collections::HashSet<String> {
    let mut retained = inventory.tagged.clone();
    retained.extend(
        inventory
            .trees
            .iter()
            .filter(|native| {
                native.as_str() == "main"
                    || crate::db::is_internal_system_branch(native)
                    || collector_retains_native_tree(plan, full_path, object_base, native)
                    || crate::branch_names::retain_unpublished_table_fork(native, |incarnation| {
                        plan.live_branch_incarnations.contains(incarnation)
                    })
            })
            .cloned(),
    );
    retained.extend(inventory.age_retained.iter().cloned());
    inventory.retain_dependencies(&mut retained);
    retained
}

/// Auxiliary files are retained only by native trees with an independent root.
pub(crate) async fn auxiliary_native_tree_roots(
    dataset: &lance::Dataset,
    full_path: &str,
    plan: &super::collector::CollectorReport,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<std::collections::HashSet<String>> {
    let inventory = native_fork_inventory(dataset, before_timestamp).await?;
    let base = dataset.branch_location().path.to_string();
    Ok(native_table_retention_roots(
        &inventory, plan, full_path, &base,
    ))
}

async fn reconcile_frozen_native_forks(
    db: &Omnigraph,
    frozen: NativeForkReconciliationPlan,
    plan: &super::collector::CollectorReport,
) -> Result<BranchReconcileStats> {
    let mut stats = frozen.stats;
    let mut classification_checked = false;
    for (table_key, full_path, object_base, inventory) in frozen.tables {
        if inventory.trees.is_empty() {
            continue;
        }
        if !classification_checked {
            classification_checked = true;
            if let Err(error) =
                fail(&CLEANUP_RESOLVE_BRANCH_SNAPSHOT).and_then(|()| fail(&CLASSIFY_FRESH_READ))
            {
                stats
                    .failures
                    .push(("__manifest".to_string(), error.to_string()));
                return Ok(stats);
            }
        }
        let retained = native_table_retention_roots(&inventory, plan, &full_path, &object_base);
        collect_native_forks(db, &full_path, &table_key, inventory, retained, &mut stats).await;
    }
    if let Some(inventory) = frozen.manifest {
        let full_path = crate::db::manifest::manifest_uri(db.root_uri());
        let mut retained = plan.protected_manifest_branches.clone();
        retained.extend(inventory.tagged.iter().cloned());
        collect_native_forks(
            db,
            &full_path,
            "__manifest",
            inventory,
            retained,
            &mut stats,
        )
        .await;
    }
    Ok(stats)
}

#[cfg(all(test, feature = "failpoints"))]
async fn reconcile_orphaned_branches_under_control_gates(
    db: &Omnigraph,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<BranchReconcileStats> {
    let frozen = prepare_native_fork_reconciliation(db, before_timestamp).await?;
    let branches = cleanup_graph_branches(db).await?;
    let plan = super::collector::plan_collection(
        db,
        &CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        },
        &branches,
    )
    .await?;
    reconcile_frozen_native_forks(db, frozen, &plan).await
}

decide_seam! {
    pub static CLEANUP_RECONCILE_FORK = ("cleanup.reconcile_fork", Unreachable, [Fail]);
}

async fn collect_native_forks(
    db: &Omnigraph,
    full_path: &str,
    table_key: &str,
    mut inventory: NativeForkInventory,
    mut retained: std::collections::HashSet<String>,
    stats: &mut BranchReconcileStats,
) {
    retained.extend(inventory.age_retained.iter().cloned());
    inventory.retain_dependencies(&mut retained);
    let protected_zombie_roots = retained
        .iter()
        .filter(|branch| !inventory.refs.contains_key(*branch))
        .filter_map(|branch| branch.split('/').next().map(str::to_string))
        .collect::<std::collections::HashSet<_>>();
    retained.extend(
        inventory
            .trees
            .iter()
            .filter(|branch| {
                branch
                    .split('/')
                    .next()
                    .is_some_and(|root| protected_zombie_roots.contains(root))
            })
            .cloned(),
    );
    inventory.retain_dependencies(&mut retained);
    let mut candidates = inventory
        .trees
        .iter()
        .filter(|branch| !retained.contains(*branch))
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    loop {
        let leaves = candidates
            .iter()
            .filter(|candidate| {
                !inventory
                    .trees
                    .iter()
                    .any(|other| inventory.depends_on(other, candidate))
            })
            .cloned()
            .collect::<Vec<_>>();
        if leaves.is_empty() {
            break;
        }
        for branch in leaves {
            candidates.remove(&branch);
            let outcome = match fail(&CLEANUP_RECONCILE_FORK) {
                Ok(()) => db.storage().force_delete_branch(full_path, &branch).await,
                Err(injected) => Err(injected),
            };
            match outcome {
                Ok(()) => {
                    inventory.trees.remove(&branch);
                    inventory.refs.remove(&branch);
                    stats.reclaimed.push((table_key.to_string(), branch));
                }
                Err(error) => {
                    stats
                        .failures
                        .push((table_key.to_string(), format!("{branch}: {error}")));
                }
            }
        }
    }
    for branch in candidates {
        stats.failures.push((
            table_key.to_string(),
            format!("cleanup retained '{branch}' because a native dependency remains"),
        ));
    }
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
    use crate::loader::LoadMode;

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
        assert!(is_retryable_lance_conflict(
            &lance::Error::commit_conflict_source(
                1,
                Box::new(std::io::Error::other("overlapping rewrite")),
            )
        ));
        assert!(!is_retryable_lance_conflict(&lance::Error::invalid_input(
            "not a conflict"
        )));
    }

    async fn node_table_uri(db: &Omnigraph, type_name: &str) -> String {
        let table_key = format!("node:{type_name}");
        let snapshot = db
            .snapshot_of(crate::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        let table_path = &snapshot
            .dataset(&table_key)
            .unwrap_or_else(|| panic!("live manifest has no registration for {table_key}"))
            .dataset_path;
        format!(
            "{}/{}",
            db.uri().trim_end_matches('/'),
            table_path.trim_start_matches('/')
        )
    }

    #[tokio::test]
    async fn reconcile_caches_live_branch_snapshot_resolution_failure() {
        let _scenario = crate::seams::FailScenario::setup();
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let schema = "node Person { name: String @key }\nnode Company { name: String @key }\n";
        let db = crate::Session::from_defaults(
            std::sync::Arc::new(Omnigraph::init(uri, schema).await.unwrap()),
            omnigraph_compiler::settings::SessionSettings::default(),
        );
        db.load_jsonl(
            "{\"type\":\"Person\",\"data\":{\"name\":\"Alice\"}}\n\
             {\"type\":\"Company\",\"data\":{\"name\":\"Acme\"}}",
            LoadMode::Merge,
        )
        .await
        .unwrap();
        db.branch_create("feature").await.unwrap();
        let feature_native = db.native_branch_for("feature").await.unwrap();

        for type_name in ["Person", "Company"] {
            let table_uri = node_table_uri(&db, type_name).await;
            // forbidden-api-allow: test synthesizes a branch ref directly on the Lance dataset.
            let mut ds = lance::Dataset::open(&table_uri).await.unwrap();
            let base = ds.version().version;
            ds.create_branch(&feature_native, base, None).await.unwrap();
        }

        let _fp = CLEANUP_RESOLVE_BRANCH_SNAPSHOT.fire_always();
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

        drop(_fp);
        db.branch_delete("feature").await.unwrap();
        let stats = reconcile_orphaned_branches(&db).await.unwrap();
        assert!(stats.failures.is_empty(), "{stats:?}");
        assert!(
            stats
                .reclaimed
                .iter()
                .any(|(table, native)| table == "__manifest" && native == &feature_native),
            "an unreferenced retired tree is reclaimable: {stats:?}"
        );
        assert!(
            !dir.path()
                .join("__manifest/_refs/branches")
                .join(format!("{feature_native}.json"))
                .exists()
        );
        assert!(
            !dir.path()
                .join("__manifest/tree")
                .join(feature_native)
                .exists()
        );
    }
    #[tokio::test]
    async fn incomplete_origin_trace_preserves_other_tables_native_trees() {
        use object_store::ObjectStoreExt;
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = crate::Session::from_defaults(
            std::sync::Arc::new(
                Omnigraph::init(
                    uri,
                    "node Person { name: String @key }\nnode Company { name: String @key }",
                )
                .await
                .unwrap(),
            ),
            omnigraph_compiler::settings::SessionSettings::default(),
        );
        db.load_jsonl(
            r#"{"type":"Person","data":{"name":"p"}}
{"type":"Company","data":{"name":"c"}}"#,
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
        let company_uri = node_table_uri(&db, "Company").await;
        // forbidden-api-allow: test-only unregistered native tree for the incomplete-origin deletion barrier
        let mut company = lance::Dataset::open(&company_uri).await.unwrap();
        let version = company.version().version;
        company
            .create_branch("unknown-origin", version, None)
            .await
            .unwrap();
        let tree = std::path::Path::new(&company_uri).join("tree/unknown-origin");
        assert!(tree.exists());
        let snapshot = db
            .snapshot_of(crate::db::ReadTarget::branch("main"))
            .await
            .unwrap();
        let person = snapshot.open_lance_dataset("node:Person").await.unwrap();
        let store = person.object_store(None).await.unwrap();
        let path = person.manifest_location().path.clone();
        let bytes = store.inner.get(&path).await.unwrap().bytes().await.unwrap();
        store.delete(&path).await.unwrap();
        let rows = db
            .cleanup(CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: None,
            })
            .await
            .unwrap();
        assert!(
            rows.iter()
                .any(|row| row.type_key == "node:Person" && row.error.is_some()),
            "{rows:?}"
        );
        assert!(
            tree.exists(),
            "an unreadable retained image may borrow this tree's files"
        );
        store.inner.put(&path, bytes.into()).await.unwrap();
        let rows = db
            .cleanup(CleanupPolicyOptions {
                keep_versions: Some(1),
                older_than: None,
            })
            .await
            .unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        assert!(
            !tree.exists(),
            "complete origin evidence allows orphan reclamation"
        );
    }
}
