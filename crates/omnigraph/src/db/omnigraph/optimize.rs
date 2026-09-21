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
use lance::dataset::cleanup::{CleanupPolicy, RemovalStats};
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

    /// Stat for a dataset skipped because the graph manifest and Lance HEAD disagree.
    fn skipped_for_drift(
        type_key: String,
        published_dataset_version: u64,
        lance_head_version: u64,
    ) -> Self {
        Self {
            type_key,
            fragments_removed: 0,
            fragments_added: 0,
            committed: false,
            skipped: Some(SkipReason::DriftNeedsRepair),
            published_dataset_version: Some(published_dataset_version),
            lance_head_version: Some(lance_head_version),
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
    /// `error` is a retention reason, not a failure: version GC was skipped for
    /// a blocked pin or an unproven detached copy, and nothing went wrong.
    pub deferred: bool,
}

struct OptimizeTableTask {
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    full_path: String,
    expected_version: u64,
    entry: crate::db::manifest::DatasetEntry,
}

struct PreparedOptimizeTable {
    identity: crate::db::manifest::TableIdentity,
    table_key: String,
    full_path: String,
    dataset_path: String,
    expected_version: u64,
    initial_snapshot: crate::storage_layer::SnapshotHandle,
}

enum OptimizePreparation {
    Work(PreparedOptimizeTable),
    Stat(DatasetOptimizeStats),
}

struct OptimizeEffectOutcome {
    stat: DatasetOptimizeStats,
    effect: Option<OptimizeTableEffect>,
}

/// One table's staged maintenance: the pin update to publish, the version it
/// was planned from, and the held promotion to run after publication.
struct OptimizeTableEffect {
    update: crate::db::DatasetUpdate,
    expected_version: u64,
    promotion: crate::db::HeldPromotion,
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
        // RFC 0067: every productive table stages its rewrite (and any
        // deferred index build) as detached versions of its pin, the batch
        // publishes once with an exact CAS on the pins it planned from, and
        // the writer promotes the versions it holds. A failure before
        // publication leaves garbage the reaper retires; one after it leaves
        // pending pins the next writer or cleanup promotes.
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
        let mut promotions = Vec::new();
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
                promotions.push(effect.promotion);
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
            match fail(&OPTIMIZE_POST_PUBLISH_PRE_PROMOTION) {
                Ok(()) => db.promote_held_all(promotions).await,
                Err(error) => {
                    tracing::warn!(error = %error, "optimize promotion interrupted; the next writer promotes")
                }
            }
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

    // Data-table publish and promotion are finished. Release the sorted table
    // and schema gates before physical internal maintenance; retain main's
    // branch gate through that maintenance.
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
        .storage()
        .open_dataset_head(&task.full_path, None)
        .await?;
    // Optimize is a graph-global writer: it promotes a pending pin before it
    // plans, so the plan runs from the linear twin, and refuses a blocked one
    // (RFC 0067).
    let snapshot = db
        .promote_pending_pin(&task.table_key, &task.full_path, &task.entry, snapshot)
        .await?;
    let lance_head_version = snapshot.version();
    if lance_head_version < task.expected_version {
        return Err(OmniError::manifest_internal(format!(
            "{} is at Lance HEAD version {}, behind published dataset version {}",
            dataset_subject(&task.table_key),
            lance_head_version,
            task.expected_version
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
            DatasetOptimizeStats::skipped_for_drift(
                task.table_key,
                task.expected_version,
                lance_head_version,
            ),
        ));
    }

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
        dataset_path: task.entry.dataset_path.clone(),
        expected_version: task.expected_version,
        initial_snapshot: snapshot,
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
    /// The batch published its pins; the held promotions have not run.
    pub static OPTIMIZE_POST_PUBLISH_PRE_PROMOTION = ("optimize.post_publish_pre_promotion", Unreachable, [Fail]);
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
    fail(&OPTIMIZE_BEFORE_COMPACT)?;
    let options = CompactionOptions::default();
    let mut chain = Vec::new();
    let mut tip = base.clone();
    let mut tip_identity = None;
    let mut metrics = CompactionMetrics::default();
    if let Some((staged, compaction_metrics)) =
        db.storage().stage_compaction(&base, &options).await?
    {
        let (rewrite, identity) = db.storage().commit_staged_detached(tip, staged).await?;
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
        let (folded, identity) = db.storage().commit_staged_detached(tip, staged).await?;
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
        let (indexed, identity) = db.storage().commit_staged_detached(tip, staged).await?;
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
        .with_staged(state.version, identity.uuid.clone());
    let promotion = crate::db::HeldPromotion {
        table_key: table_key.clone(),
        dataset_path: work.dataset_path,
        full_path: full_path.clone(),
        table_branch: None,
        base,
        chain,
        detached: tip,
        target: published_dataset_version,
        uuid: identity.uuid,
        e_tag: version_metadata.e_tag().map(str::to_string),
    };
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
            promotion,
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

/// Remove any stored `lance.auto_cleanup.*` config from a table so compaction
/// stays **non-destructive by construction**. Used by the internal-table path
/// ([`compact_internal_table`]), whose `compact_files` commits linearly through
/// Lance's own hook; data tables need no strip, since every engine commit and
/// promotion of a detached rewrite skips auto-cleanup (RFC 0067).
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
/// The internal-table path needs no crash protocol for it: it commits at HEAD
/// and is read at HEAD — the strip is a content-preserving config commit, so a crash
/// leaves the table readable and content-identical, see [`compact_internal_table`].
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

    // Writers take schema, then branch, then table gates. Cleanup takes the
    // conservative superset and holds it through every `cleanup_old_versions`
    // call, so no in-process writer stages or promotes while versions are
    // collected.
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

    // Lance protects versions referenced by its own per-dataset branches, but
    // an OmniGraph branch is lazy: until a table is first written on that
    // branch its manifest entry points directly at an older MAIN version and
    // no Lance branch ref exists on the data table. Resolve every live graph
    // branch from fresh authority while schema + all branch/table gates are
    // held, then cap each main dataset's GC cutoff at its oldest such pin.
    // Main itself participates: its manifest-visible version must open and
    // equal Lance HEAD unless its chain is recognized as blocked, so drift
    // outside those chains must be resolved before cleanup rather
    // than letting HEAD-based GC collect graph-visible authority.
    // Any branch snapshot read failure aborts before the first table GC: an
    // unknown live reference is never evidence that a version is disposable.
    let mut oldest_live_main_version_by_path = std::collections::HashMap::<String, u64>::new();
    // RFC 0067: a table whose pin a foreign commit blocks keeps its
    // acknowledged rows only in a detached version, which stock version GC
    // does not protect. Skip GC on it, and protect every link of its chain
    // from the detached-manifest reaper below.
    let mut blocked_gc_locations = std::collections::HashSet::<String>::new();
    let mut protected_detached =
        std::collections::HashMap::<String, std::collections::HashSet<u64>>::new();
    let mut table_locations = std::collections::BTreeSet::<(String, Option<String>)>::new();
    let mut published_pins =
        std::collections::HashMap::<String, Vec<crate::db::manifest::DatasetEntry>>::new();
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
        for entry in crate::db::manifest::ManifestCoordinator::table_versions_under_control_gates(
            db.root_uri(),
            branch_target.as_deref(),
            &db.control_session(),
        )
        .await?
        {
            let full_path = format!("{}/{}", db.root_uri(), entry.dataset_path);
            let location = super::promotion::table_location(
                &full_path,
                entry.native_dataset_branch.as_deref(),
            );
            published_pins.entry(location).or_default().push(entry);
        }
        for entry in branch_snapshot.datasets() {
            // RFC 0067: a pending pin is promoted before any version is
            // reclaimed, so stock Lance cleanup only ever sees linear history
            // and the pin's detached manifest becomes surplus.
            let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
            table_locations.insert((full_path.clone(), entry.native_dataset_branch.clone()));
            if let (Some(staged), Some(uuid)) = (
                entry.version_metadata.staged_version(),
                entry.version_metadata.transaction_uuid(),
            ) && let Some(chain) = settle_pin_before_cleanup(db, entry, staged, uuid).await?
            {
                let location = super::promotion::table_location(
                    &full_path,
                    entry.native_dataset_branch.as_deref(),
                );
                blocked_gc_locations.insert(location.clone());
                protected_detached
                    .entry(location)
                    .or_default()
                    .extend(chain);
            }
            // Validate that the exact protected version is still openable
            // before GC starts. This catches pre-existing damage from an older
            // cleanup implementation and keeps the sweep fail-closed instead
            // of deleting unrelated history around an already-broken branch.
            entry.open(db.root_uri(), None).await.map_err(|err| {
                OmniError::manifest_conflict(format!(
                    "cleanup could not classify live branch '{branch_label}' {} at published dataset version {}; refusing version GC: {err}",
                    dataset_subject(&entry.type_key), entry.published_dataset_version
                ))
            })?;
            if entry.native_dataset_branch.is_some() {
                continue;
            }
            if branch_target.is_none() && !blocked_gc_locations.contains(&full_path) {
                let head = db.storage().open_dataset_head(&full_path, None).await?;
                if head.version() != entry.published_dataset_version {
                    return Err(OmniError::manifest_conflict(format!(
                        "cleanup found uncovered HEAD drift for {}: published dataset version {}, \
                         Lance HEAD version {}; run `omnigraph repair` before version GC",
                        dataset_subject(&entry.type_key),
                        entry.published_dataset_version,
                        head.version()
                    )));
                }
            }
            oldest_live_main_version_by_path
                .entry(full_path)
                .and_modify(|oldest| *oldest = (*oldest).min(entry.published_dataset_version))
                .or_insert(entry.published_dataset_version);
        }
    }

    let now = crate::dst_clock::now_utc();
    let before_timestamp = options.older_than.map(|d| now - d);
    let deferred_gc = reap_detached_manifests(
        db,
        &table_locations,
        &protected_detached,
        &published_pins,
        before_timestamp,
    )
    .await;
    let reconciled =
        reconcile_orphaned_branches_under_control_gates(db, before_timestamp, &deferred_gc).await?;
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
    let blocked_gc_locations = &blocked_gc_locations;
    let deferred_gc = &deferred_gc;

    // Fault-isolated per table: a single table's GC failure is recorded on its
    // stats row (`error: Some`) and logged, never aborting the healthy tables.
    // cleanup is the convergence backstop, so it must do as much as it can and
    // converge on re-run rather than fail wholesale (invariant 13).
    let results: Vec<DatasetCleanupStats> = futures::stream::iter(table_tasks)
        .map(|(table_key, full_path, live_main_floor)| async move {
            let outcome: Result<std::result::Result<RemovalStats, String>> = async {
                fail(&CLEANUP_TABLE_GC)?;
                if blocked_gc_locations.contains(&full_path) {
                    return Ok(Err(
                        "a published write's promotion is blocked by a foreign commit at its \
                         target version; version GC is skipped for this table; `omnigraph \
                         repair` reports the block and nothing resolves a blocked pin yet; \
                         reads, mutations and loads continue"
                            .to_string(),
                    ));
                }
                match deferred_gc.get(&full_path) {
                    Some(GcSkip::Retained(reason)) => return Ok(Err(reason.clone())),
                    Some(GcSkip::Failed(reason)) => {
                        return Err(OmniError::manifest_conflict(reason.clone()));
                    }
                    None => {}
                }
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
                    // Only version numbers are needed: avoid fetching every
                    // historical manifest and its summary metadata.
                    let versions = ds.version_refs().await.map_err(OmniError::storage)?;
                    let retain = (keep as usize).max(1);
                    let cutoff = if versions.len() <= retain {
                        versions.first()
                    } else {
                        versions.get(versions.len() - retain)
                    }
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "cleanup found no versions for open {}",
                            dataset_subject(&table_key)
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
                    .map(Ok)
                    .map_err(OmniError::storage)
            }
            .await;
            match outcome {
                Ok(Ok(removed)) => DatasetCleanupStats {
                    type_key: table_key,
                    bytes_removed: removed.bytes_removed,
                    old_versions_removed: removed.old_versions,
                    error: None,
                    deferred: false,
                },
                Ok(Err(reason)) => {
                    tracing::info!(
                        target: "omnigraph::cleanup",
                        table = %table_key,
                        reason,
                        "version GC deferred for dataset",
                    );
                    DatasetCleanupStats {
                        type_key: table_key,
                        bytes_removed: 0,
                        old_versions_removed: 0,
                        error: Some(reason),
                        deferred: true,
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        target: "omnigraph::cleanup",
                        table = %table_key,
                        error = %err,
                        "version GC failed for dataset; other datasets unaffected",
                    );
                    DatasetCleanupStats {
                        type_key: table_key,
                        bytes_removed: 0,
                        old_versions_removed: 0,
                        error: Some(err.to_string()),
                        deferred: false,
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
    reconcile_orphaned_branches_under_control_gates(db, None, &std::collections::HashMap::new())
        .await
}

pub(super) async fn cleanup_graph_branches(db: &Omnigraph) -> Result<Vec<Option<String>>> {
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
    let refs = crate::branch_control::list_all_branch_contents(dataset).await?;
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
            let expected = refs
                .keys()
                .map(|name| (directory.clone().join(format!("{name}.json")), name))
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
        if let Some(missing) = refs.keys().find(|name| !observed_refs.contains(*name)) {
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

async fn reconcile_orphaned_branches_under_control_gates(
    db: &Omnigraph,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    deferred_gc: &std::collections::HashMap<String, GcSkip>,
) -> Result<BranchReconcileStats> {
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
    let table_targets = registrations.into_iter().map(|registration| {
        let full_path = format!("{}/{}", db.root_uri, registration.table_path);
        (registration.identity, registration.table_key, full_path)
    });
    let mut stats = BranchReconcileStats::default();
    let mut references = None;
    let storage = db.storage();
    for (identity, table_key, full_path) in table_targets {
        if let Some(skip) = deferred_gc.get(&full_path) {
            stats.failures.push((table_key, skip.reason().to_string()));
            continue;
        }
        let inventory = async {
            let handle = match storage.open_dataset_head(&full_path, None).await {
                Ok(handle) => handle,
                Err(error)
                    if !live_identities.contains(&identity)
                        && error.storage_failure().is_some_and(|failure| {
                            failure.kind == omnigraph_storage::StorageFailureKind::NotFound
                        }) =>
                {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            native_fork_inventory(handle.dataset(), before_timestamp)
                .await
                .map(Some)
        }
        .await;
        let inventory = match inventory {
            Ok(Some(inventory)) => inventory,
            Ok(None) => continue,
            Err(error) => {
                stats.failures.push((table_key.clone(), error.to_string()));
                continue;
            }
        };
        if inventory.trees.is_empty() {
            continue;
        }
        if references.is_none() {
            let captured = match fail(&CLEANUP_RESOLVE_BRANCH_SNAPSHOT).and_then(|()| fail(&CLASSIFY_FRESH_READ)) {
                Ok(()) => {
                    crate::db::manifest::ManifestCoordinator::native_fork_references_under_control_gates(
                        db.root_uri(),
                        &db.control_session(),
                    )
                    .await
                }
                Err(error) => Err(error),
            };
            match captured {
                Ok(captured) => references = Some(captured),
                Err(error) => {
                    stats
                        .failures
                        .push(("__manifest".to_string(), error.to_string()));
                    return Ok(stats);
                }
            }
        }
        let references = references
            .as_ref()
            .expect("native trees require live roots");
        let mut retained = inventory.tagged.clone();
        retained.extend(
            inventory
                .trees
                .iter()
                .filter(|native| {
                    native.as_str() == "main"
                        || crate::db::is_internal_system_branch(native)
                        || references.contains_tree(identity, native)
                        || references.retains_unpublished_fork(native)
                })
                .cloned(),
        );
        collect_native_forks(db, &full_path, &table_key, inventory, retained, &mut stats).await;
    }
    reconcile_retired_manifest_forks(db, before_timestamp, &mut stats).await;
    Ok(stats)
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

async fn reconcile_retired_manifest_forks(
    db: &Omnigraph,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    stats: &mut BranchReconcileStats,
) {
    let full_path = crate::db::manifest::manifest_uri(db.root_uri());
    let captured = async {
        let handle = db.storage().open_dataset_head(&full_path, None).await?;
        let mut retained =
            crate::branch_control::list_live_manifest_branch_contents(handle.dataset())
                .await?
                .into_keys()
                .collect::<std::collections::HashSet<_>>();
        let inventory = native_fork_inventory(handle.dataset(), before_timestamp).await?;
        retained.extend(inventory.tagged.iter().cloned());
        Ok::<_, OmniError>((inventory, retained))
    }
    .await;
    match captured {
        Ok((inventory, retained)) => {
            collect_native_forks(db, &full_path, "__manifest", inventory, retained, stats).await;
        }
        Err(error) => stats
            .failures
            .push(("__manifest".to_string(), error.to_string())),
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
    }
}

decide_seam! {
    /// In cleanup, after a pending pin was promoted and before its detached
    /// manifest is deleted (RFC 0067). A failure here leaves a promoted pin
    /// whose detached manifest the next cleanup reaps.
    pub static CLEANUP_PRE_REAP = ("cleanup.pre_reap", Unreachable, [Fail]);
}

/// Promote one pending pin, then delete its detached manifest once the
/// pin is linear (RFC 0067). Returns the detached versions of a blocked
/// pin's chain, which cleanup must neither GC around nor reap.
async fn settle_pin_before_cleanup(
    db: &Omnigraph,
    entry: &crate::db::manifest::DatasetEntry,
    staged: u64,
    uuid: &str,
) -> Result<Option<Vec<u64>>> {
    let full_path = format!("{}/{}", db.root_uri, entry.dataset_path);
    let table_branch = entry.native_dataset_branch.as_deref();
    let location = super::promotion::table_location(&full_path, table_branch);
    let outcome = super::promotion::promote_pin(
        db,
        &entry.type_key,
        &full_path,
        table_branch,
        entry.published_dataset_version,
        staged,
        uuid,
    )
    .await?;
    match outcome {
        super::promotion::Promotion::Promoted(_) | super::promotion::Promotion::AlreadyPromoted => {
            // Keep the chain intact until historical pins have supplied the
            // UUID proof for every candidate. Reaping happens in one pass below.
            fail(&CLEANUP_PRE_REAP)?;
            Ok(None)
        }
        super::promotion::Promotion::Blocked(reason) => {
            tracing::warn!(
                table = entry.type_key.as_str(),
                target = entry.published_dataset_version,
                reason,
                "cleanup found a blocked pin; version GC is skipped for its table"
            );
            let (_, chain) = super::promotion::walk_chain(db, &location, staged).await?;
            Ok(Some(
                chain.into_iter().map(|(version, _)| version).collect(),
            ))
        }
    }
}

decide_seam! {
    /// In cleanup, before each proven detached manifest is deleted (RFC 0067).
    /// A failure here leaves the chain's tip for the next cleanup to re-prove.
    pub static CLEANUP_REAP_DELETE = ("cleanup.reap_delete", Unreachable, [Fail]);
}

/// Why a detached copy keeps its table out of version GC.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetainedDetached {
    /// A link of a blocked pin's chain: acknowledged rows live only there.
    PendingPin,
    /// No published pin proves a linear twin carries the copy's transaction.
    NoTwinProof,
}

/// The deferral reason of one table: every retained copy with its cause.
fn retained_detached_reason(mut retained: Vec<(u64, RetainedDetached)>) -> String {
    retained.sort_unstable();
    let listed = retained
        .iter()
        .take(8)
        .map(|(version, why)| match why {
            RetainedDetached::PendingPin => format!("{version} (blocked pin)"),
            RetainedDetached::NoTwinProof => format!("{version} (no twin proof)"),
        })
        .collect::<Vec<_>>()
        .join(", ");
    let more = retained.len().saturating_sub(8);
    let suffix = if more == 0 {
        String::new()
    } else {
        format!(" and {more} more")
    };
    format!(
        "detached versions remain: {listed}{suffix}; version GC is skipped to preserve their data"
    )
}

/// Reap only detached copies with a UUID-verified twin of a published pin.
/// Every retained detached manifest also retains its data: stock Lance GC does
/// not trace detached references, even with `delete_unverified: false`.
/// Why one table's version GC is skipped for this run.
enum GcSkip {
    /// Copies are retained by rule; nothing went wrong.
    Retained(String),
    /// The reaper could not finish its proof or a delete.
    Failed(String),
}

impl GcSkip {
    fn reason(&self) -> &str {
        match self {
            Self::Retained(reason) | Self::Failed(reason) => reason,
        }
    }
}

async fn reap_detached_manifests(
    db: &Omnigraph,
    locations: &std::collections::BTreeSet<(String, Option<String>)>,
    protected: &std::collections::HashMap<String, std::collections::HashSet<u64>>,
    published: &std::collections::HashMap<String, Vec<crate::db::manifest::DatasetEntry>>,
    before_timestamp: Option<chrono::DateTime<chrono::Utc>>,
) -> std::collections::HashMap<String, GcSkip> {
    let mut deferred = std::collections::HashMap::new();
    for (full_path, table_branch) in locations {
        let result: Result<Vec<(u64, RetainedDetached)>> = async {
            let location = super::promotion::table_location(full_path, table_branch.as_deref());
            let handle = db
                .storage()
                .open_dataset_head(full_path, table_branch.as_deref())
                .await?;
            let dataset = handle.dataset();
            let store = dataset
                .object_store(None)
                .await
                .map_err(OmniError::storage)?;
            let mut files = store.read_dir_all(&dataset.versions_dir(), None);
            let mut candidates = std::collections::HashMap::new();
            while let Some(file) = files.next().await {
                let file = file.map_err(OmniError::storage)?;
                let Some(version) = file
                    .location
                    .filename()
                    .and_then(|name| name.strip_prefix('d'))
                    .and_then(|name| name.strip_suffix(".manifest"))
                    .and_then(|name| name.parse::<u64>().ok())
                else {
                    continue;
                };
                candidates.insert(version, file.last_modified);
            }
            // Historical pins outlive their detached copies. Verify only copies
            // still present, rather than reopening every old linear version.
            let mut redundant = std::collections::HashSet::new();
            let mut proven_chains = Vec::new();
            let mut seen = std::collections::HashSet::new();
            for entry in published.get(&location).into_iter().flatten() {
                let key = (
                    entry.version_metadata.staged_version(),
                    entry.published_dataset_version,
                );
                if key
                    .0
                    .is_some_and(|version| candidates.contains_key(&version))
                    && seen.insert(key)
                {
                    let mut chain = super::promotion::promoted_chain_versions(db, entry).await?;
                    chain.reverse();
                    redundant.extend(chain.iter().copied());
                    proven_chains.push(chain);
                }
            }
            let is_protected = |version: &u64| {
                protected
                    .get(&location)
                    .is_some_and(|versions| versions.contains(version))
            };
            let retained = candidates
                .keys()
                .filter_map(|version| {
                    if is_protected(version) {
                        Some((*version, RetainedDetached::PendingPin))
                    } else if !redundant.contains(version) {
                        Some((*version, RetainedDetached::NoTwinProof))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            for chain in proven_chains {
                for version in chain {
                    let Some(last_modified) = candidates.get(&version).copied() else {
                        continue;
                    };
                    if is_protected(&version)
                        || before_timestamp.is_some_and(|cutoff| last_modified >= cutoff)
                    {
                        break;
                    }
                    fail(&CLEANUP_REAP_DELETE)?;
                    let detached = super::promotion::detached_manifest_path(&location, version);
                    db.storage_adapter().delete(&detached).await?;
                    candidates.remove(&version);
                }
            }
            Ok(retained)
        }
        .await;
        let skip = match result {
            Ok(retained) if retained.is_empty() => continue,
            Ok(retained) => GcSkip::Retained(retained_detached_reason(retained)),
            Err(error) => GcSkip::Failed(format!(
                "could not prove detached manifests reclaimable; version GC is skipped: {error}"
            )),
        };
        deferred.insert(full_path.clone(), skip);
    }
    deferred
}
