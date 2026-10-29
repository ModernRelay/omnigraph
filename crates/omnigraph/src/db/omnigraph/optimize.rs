//! Lance compaction + version cleanup exposed at the graph level.
//!
//! Lance accumulates many small `.lance` fragment files per backing dataset over the
//! life of a graph: each `write`, `load`, and `change` op appends one or more
//! fragments and a new manifest. Over long timescales this hurts open times
//! and S3 object counts without improving anything.
//!
//! Two dials:
//!
//! * `optimize_all_datasets` — stages Lance compaction on every dataset as
//!   detached commits and **publishes them together in one `__manifest`
//!   batch**, so each registration's pin names the compacted version (reads
//!   pin the published dataset version, so without the publish compaction
//!   would be invisible to readers). Compaction is content-preserving (Lance
//!   `Operation::Rewrite` "reorganizes data without semantic modification"),
//!   so old fragments remain reachable through older `__manifest` versions
//!   until `cleanup` stops retaining them.
//! * `cleanup_all_datasets` — the engine's tracing collector
//!   (`collector.rs`); stock `cleanup_old_versions` is never called on a
//!   graph table. `--keep` / `--older-than` choose the retained `__manifest`
//!   versions per live branch; the collector keeps every file those
//!   versions, tags and merge bases reach and deletes the rest. Destructive
//!   to unreferenced version history — callers should gate this behind an
//!   explicit confirm flag at the CLI layer.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::stream::{FuturesOrdered, FuturesUnordered, StreamExt};
use lance::dataset::optimize::{CompactionMetrics, CompactionOptions};
use tokio::time::Instant;

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

/// What one optimize run did to one table. A run reports exactly one outcome
/// for every table it considered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TableOptimizeOutcome {
    /// A rewrite or index link was staged and is in the run's publication.
    Optimized,
    /// Planning found nothing to do.
    Unchanged,
    /// Not started: nothing was written for the table.
    Deferred(DeferReason),
    /// Deliberately left alone.
    Skipped(SkipReason),
}

impl TableOptimizeOutcome {
    /// Stable machine-readable token for serialized output. Once emitted this
    /// is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            TableOptimizeOutcome::Optimized => "optimized",
            TableOptimizeOutcome::Unchanged => "unchanged",
            TableOptimizeOutcome::Deferred(_) => "deferred",
            TableOptimizeOutcome::Skipped(_) => "skipped",
        }
    }
}

/// Why a run did not start a table. Typed so callers branch on the reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DeferReason {
    /// The run had held main's writer gates for `max_gate_hold` when the
    /// table's turn came. This table and every later one wait for a later
    /// run, which [`OptimizeReport::resume_at`] starts at.
    GateHoldLimit,
}

impl DeferReason {
    /// Stable machine-readable token for serialized output. Once emitted this
    /// is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            DeferReason::GateHoldLimit => "gate_hold_limit",
        }
    }
}

impl std::fmt::Display for DeferReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DeferReason::GateHoldLimit => "the run reached its writer-gate hold limit",
        })
    }
}

/// Per-table outcome of `optimize_all_datasets`. This is a returned result type,
/// not built by callers, so it is `#[non_exhaustive]`: future fields stay
/// non-breaking and downstream code reads fields rather than constructing it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DatasetOptimizeStats {
    /// Qualified graph type key.
    pub type_key: String,
    /// What the run did to this table.
    pub outcome: TableOptimizeOutcome,
    /// Number of source fragments that were rewritten by Lance.
    pub fragments_removed: usize,
    /// Number of new, larger fragments Lance produced.
    pub fragments_added: usize,
    /// Index work deferred this run, with the reason and remedy: a vector
    /// property without trainable vectors, or full-text coverage requiring an
    /// explicit rebuild. Deferred work alone does not stage or publish.
    pub pending_indexes: Vec<super::PendingIndex>,
    /// Time from the start of the table's planning to the end of its staging;
    /// zero for a table the run did not start.
    pub duration: Duration,
}

impl DatasetOptimizeStats {
    fn planned(type_key: String, metrics: &CompactionMetrics, committed: bool) -> Self {
        Self {
            type_key,
            outcome: if committed {
                TableOptimizeOutcome::Optimized
            } else {
                TableOptimizeOutcome::Unchanged
            },
            fragments_removed: metrics.fragments_removed,
            fragments_added: metrics.fragments_added,
            pending_indexes: Vec::new(),
            duration: Duration::ZERO,
        }
    }

    fn deferred(type_key: String, reason: DeferReason) -> Self {
        Self {
            type_key,
            outcome: TableOptimizeOutcome::Deferred(reason),
            fragments_removed: 0,
            fragments_added: 0,
            pending_indexes: Vec::new(),
            duration: Duration::ZERO,
        }
    }

    /// Whether this table advanced to a new Lance version that this run
    /// published through the graph manifest: compaction, index maintenance or
    /// materialization.
    pub fn committed(&self) -> bool {
        self.outcome == TableOptimizeOutcome::Optimized
    }

    /// `Some(reason)` if this table was deliberately not compacted.
    pub fn skipped(&self) -> Option<SkipReason> {
        match self.outcome {
            TableOptimizeOutcome::Skipped(reason) => Some(reason),
            _ => None,
        }
    }
}

/// The configuration clear that follows a run's publication under the writer
/// gates: stale `lance.auto_cleanup.*` keys on `__manifest` are removed with
/// one config-only commit. It never fails the run; the next run clears keys
/// that are still present.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigurationClear {
    /// The keys were removed.
    Cleared,
    /// No key was present.
    NothingToClear,
    /// The keys are still present.
    Failed(String),
    /// Whether the removal committed has no definite answer.
    Unknown(String),
}

impl ConfigurationClear {
    /// Stable machine-readable token for serialized output. Once emitted this
    /// is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConfigurationClear::Cleared => "cleared",
            ConfigurationClear::NothingToClear => "nothing_to_clear",
            ConfigurationClear::Failed(_) => "failed",
            ConfigurationClear::Unknown(_) => "unknown",
        }
    }
}

/// The adjacency artifact refresh, after the writer gates are released. The
/// artifact is derived state: a stale or missing one is rejected on load and
/// rebuilt in memory, so no outcome here changes an answer or fails the run.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ArtifactRefresh {
    /// A new artifact was written.
    Refreshed,
    /// No edge table advanced and an artifact exists.
    Unchanged,
    /// Deliberately not built.
    Skipped(ArtifactSkip),
    /// The build or the write failed; traversals keep building in memory.
    Failed(String),
}

impl ArtifactRefresh {
    /// Stable machine-readable token for serialized output. Once emitted this
    /// is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactRefresh::Refreshed => "refreshed",
            ArtifactRefresh::Unchanged => "unchanged",
            ArtifactRefresh::Skipped(_) => "skipped",
            ArtifactRefresh::Failed(_) => "failed",
        }
    }
}

/// Why a run did not build the adjacency artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ArtifactSkip {
    /// The catalog declares no edge type.
    NoEdgeTypes,
    /// A declared edge type has no table yet, so the artifact has no complete
    /// identity to stamp.
    IncompleteEdgeTables,
}

impl ArtifactSkip {
    /// Stable machine-readable token for serialized output. Once emitted this
    /// is part of the output contract — keep it stable.
    pub fn as_str(&self) -> &'static str {
        match self {
            ArtifactSkip::NoEdgeTypes => "no_edge_types",
            ArtifactSkip::IncompleteEdgeTables => "incomplete_edge_tables",
        }
    }
}

/// What one optimize run did. A returned result type, so `#[non_exhaustive]`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OptimizeReport {
    /// main's graph head as the run captured it under the writer gates: the
    /// parent of the commit the run publishes.
    pub input_commit: Option<String>,
    /// The `__manifest` version of that head.
    pub input_manifest_version: u64,
    /// The graph commit the run published; `None` when it staged no table.
    pub graph_commit_id: Option<String>,
    /// One entry per table the run considered, in the order it visited them.
    pub tables: Vec<DatasetOptimizeStats>,
    /// The first table a full-catalog run deferred because of a run-wide
    /// bound. A full-catalog run that passes it as `start_at` visits it
    /// first. `None` when the run stopped on no bound, and for a `tables`
    /// request, which neither reads nor moves the resume position.
    pub resume_at: Option<String>,
    /// How long the run held main's writer gates, from acquisition to release.
    pub gate_hold: Duration,
    /// The configuration clear, under the gates after the publication.
    pub configuration_clear: ConfigurationClear,
    /// The adjacency artifact refresh, after the gates are released.
    pub artifact: ArtifactRefresh,
}

/// How many tables a run plans ahead of the ones it rewrites, and how many it
/// rewrites at once. Planning only reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OptimizeConcurrency {
    pub planning: usize,
    pub rewrite: usize,
}

impl OptimizeConcurrency {
    pub fn new(planning: usize, rewrite: usize) -> Self {
        Self { planning, rewrite }
    }

    /// The direct path's setting: both from `OMNIGRAPH_MAINTENANCE_CONCURRENCY`
    /// (default 8).
    pub fn from_env() -> Self {
        let concurrency = maint_concurrency();
        Self::new(concurrency, concurrency)
    }
}

/// Bounds on one run. An absent limit does not bound it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OptimizeLimits {
    /// Stop starting tables once the run has held main's writer gates this
    /// long. The first table always starts, so every run makes progress; the
    /// writer wait is at most this, plus that table, the publication and the
    /// configuration clear.
    pub max_gate_hold: Option<Duration>,
}

impl OptimizeLimits {
    pub fn with_max_gate_hold(mut self, hold: Duration) -> Self {
        self.max_gate_hold = Some(hold);
        self
    }
}

/// What a run's before-publish hook receives.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OptimizePublication {
    /// The nonce of the commit's lineage intent. History stores the commit
    /// under an id chosen per publish attempt (`hb1.{block}.{slot}.{nonce}`);
    /// `omnigraph_core::graph_commit_id::commit_id_answers` matches such an id
    /// to this nonce.
    pub nonce: String,
    /// The graph head every publish attempt reads: the commit's parent.
    pub input_commit: Option<String>,
    /// The `__manifest` version of that head.
    pub input_manifest_version: u64,
    /// One outcome for every table the run considered, as the report lists them.
    pub tables: Vec<DatasetOptimizeStats>,
}

/// Called once, after the run's tables are staged and prepared and before the
/// publisher's first attempt; a run that stages no table never calls it. An
/// error publishes nothing and fails the run with `Unpublished` evidence.
#[derive(Clone)]
pub struct OptimizeBeforePublish(
    Arc<dyn Fn(OptimizePublication) -> BoxFuture<'static, Result<()>> + Send + Sync>,
);

impl OptimizeBeforePublish {
    pub fn new<F, Fut>(hook: F) -> Self
    where
        F: Fn(OptimizePublication) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        Self(Arc::new(move |publication| Box::pin(hook(publication))))
    }
}

impl std::fmt::Debug for OptimizeBeforePublish {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("OptimizeBeforePublish(..)")
    }
}

/// What one optimize run is asked to do. The default is the direct run as it
/// has always been: every table on main, no bound, and both concurrencies from
/// `OMNIGRAPH_MAINTENANCE_CONCURRENCY`.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct OptimizeRequest {
    /// Only these table keys (`node:<Type>`, `edge:<Type>`), in this order.
    /// A key that names no declared type is refused before any effect.
    pub tables: Option<Vec<String>>,
    /// For a full-catalog run: start at this table key, or at the first key
    /// after it when no table has it, and wrap around, so the run visits every
    /// table at most once.
    pub start_at: Option<String>,
    pub limits: OptimizeLimits,
    /// `None` reads [`OptimizeConcurrency::from_env`].
    pub concurrency: Option<OptimizeConcurrency>,
    pub before_publish: Option<OptimizeBeforePublish>,
}

impl OptimizeRequest {
    pub fn with_tables<I, S>(mut self, tables: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.tables = Some(tables.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_start_at(mut self, table_key: impl Into<String>) -> Self {
        self.start_at = Some(table_key.into());
        self
    }

    pub fn with_limits(mut self, limits: OptimizeLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_concurrency(mut self, concurrency: OptimizeConcurrency) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    pub fn with_before_publish(mut self, hook: OptimizeBeforePublish) -> Self {
        self.before_publish = Some(hook);
        self
    }

    /// The tables this request visits, in order, out of the catalog's sorted
    /// keys. Refuses a malformed request before any effect.
    fn visit_order(&self, table_keys: &[String]) -> Result<Vec<String>> {
        if let Some(concurrency) = self.concurrency
            && (concurrency.planning == 0 || concurrency.rewrite == 0)
        {
            return Err(OmniError::manifest(
                "optimize concurrency must be at least 1 for planning and rewriting",
            ));
        }
        let Some(tables) = &self.tables else {
            let start = self
                .start_at
                .as_ref()
                .map_or(0, |start| table_keys.partition_point(|key| key < start));
            let mut order = table_keys[start..].to_vec();
            order.extend_from_slice(&table_keys[..start]);
            return Ok(order);
        };
        if self.start_at.is_some() {
            return Err(OmniError::manifest(
                "optimize takes a start position only for a full-catalog run, not with a table list",
            ));
        }
        if tables.is_empty() {
            return Err(OmniError::manifest("optimize table list is empty"));
        }
        let mut seen = std::collections::BTreeSet::new();
        for table in tables {
            if table_keys.binary_search(table).is_err() {
                return Err(OmniError::manifest(format!(
                    "optimize table '{table}' names no node or edge type"
                )));
            }
            if !seen.insert(table.as_str()) {
                return Err(OmniError::manifest(format!(
                    "optimize table '{table}' is listed twice"
                )));
            }
        }
        Ok(tables.clone())
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

/// Run Lance maintenance across the node + edge datasets on `main` that
/// `request` selects, under one graph visibility envelope. Tables are visited
/// in order: up to `concurrency.planning` are planned ahead of the ones being
/// rewritten, rewrites start in visit order, and every productive dataset
/// stages detached. The batch publishes once with an exact pin CAS, so one
/// public Optimize produces at most one graph commit. Once the run has held
/// the writer gates for `max_gate_hold`, it starts no further table and
/// reports the rest deferred. The publisher owns main's `__manifest` fragment
/// layout; maintenance only removes obsolete automatic-cleanup configuration
/// from that dataset, under the gates, and refreshes the adjacency artifact
/// after releasing them.
pub async fn optimize_all_datasets(
    db: &Omnigraph,
    actor: Option<&str>,
    request: OptimizeRequest,
) -> Result<OptimizeReport> {
    db.enforce(
        omnigraph_policy::PolicyAction::Maintain,
        &omnigraph_policy::ResourceScope::Branch("main".to_string()),
        actor,
    )?;
    let concurrency = request
        .concurrency
        .unwrap_or_else(OptimizeConcurrency::from_env);
    let _export_exclusion = db.reserve_export_destructive_control()?;
    db.ensure_schema_state_valid().await?;

    // Capture complete graph authority before entering any writer gate, then
    // revalidate it after schema -> main -> table acquisition. A concurrent
    // graph or schema publish therefore refuses this attempt before any
    // physical maintenance effect.
    let authority_txn = db.open_write_txn(None).await?;
    fail(&OPTIMIZE_POST_AUTHORITY_CAPTURE_PRE_GATES)?;

    // Canonical writer order: schema -> branch -> sorted tables. Planning reads
    // catalog index intent, so it must use an operation-local accepted catalog
    // under the same schema gate as schema apply and the exact RFC-022 writers.
    let schema_permit = db.write_queue().acquire_schema_shared().await;
    db.refresh_coordinator_only().await?;
    let catalog = db.load_accepted_catalog_with_schema_gate_held().await?;
    let table_keys = all_table_keys(&catalog);
    let visit_order = request
        .visit_order(&table_keys)
        .map_err(OmniError::before_effect)?;
    let full_catalog = request.tables.is_none();

    // Optimize's one visibility point advances main's graph head, so its
    // authority is branch-wide even though physical effects are table-local.
    let main_branch_guard = db.write_queue().acquire_branch(None).await;
    let queue_keys = table_keys
        .iter()
        .map(|table_key| (table_key.clone(), None))
        .collect::<Vec<_>>();
    let table_guards = db.write_queue().acquire_many(&queue_keys).await;
    let gates_acquired = Instant::now();

    let snapshot = db.revalidate_write_txn(&authority_txn).await?;
    let input_commit = authority_txn.authority.graph_head.clone();
    let input_manifest_version = authority_txn.base.graph_manifest_version();

    let witness = authority_txn.authority.staging_witness()?;
    let table_tasks = visit_order
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

    let deadline = request
        .limits
        .max_gate_hold
        .map(|hold| gates_acquired + hold);
    let visited = run_table_pipeline(db, &catalog, table_tasks, concurrency, deadline).await?;

    let mut updates = Vec::new();
    let mut expected_versions = crate::db::manifest::ExpectedTableVersions::new();
    let mut tables = Vec::with_capacity(visited.tables.len());
    for (stat, effect) in visited.tables {
        if let Some(effect) = effect {
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
        tables.push(stat);
    }
    let edge_tables_committed = updates
        .iter()
        .any(|update| update.type_key.starts_with("edge:"));
    let mut graph_commit_id = None;
    if !updates.is_empty() {
        fail(&OPTIMIZE_POST_PHASE_B_PRE_MANIFEST_COMMIT).map_err(OmniError::unpublished)?;
        let lineage = db
            .new_lineage_intent_for_branch(None, actor, HistoryReleaseBytes::PRODUCTION)
            .await
            .map_err(OmniError::unpublished)?;
        let before_publish = request.before_publish.as_ref().map(|hook| {
            let hook = hook.0.clone();
            let tables = tables.clone();
            move |intent: super::table_ops::PublicationIntent| {
                hook(OptimizePublication {
                    nonce: intent.nonce,
                    input_commit: intent.input_commit,
                    input_manifest_version: intent.input_manifest_version,
                    tables: tables.clone(),
                })
            }
        });
        let commit = super::table_ops::commit_updates_on_branch_with_expected(
            db,
            None,
            &updates,
            &expected_versions,
            actor,
            &authority_txn,
            lineage,
            super::table_ops::Publication::Publish,
            before_publish
                .as_ref()
                .map(|hook| hook as super::table_ops::BeforePublishHook<'_>),
        )
        .await?;
        graph_commit_id = Some(commit.graph_commit_id);
        db.runtime_cache.invalidate_all().await;
        if edge_tables_committed {
            db.invalidate_graph_index().await;
        }
    }

    // The graph commit is durable from here: nothing below fails the run.
    let configuration_clear = clear_manifest_auto_cleanup(db).await;
    let gate_hold = gates_acquired.elapsed();
    drop(table_guards);
    drop(main_branch_guard);
    drop(schema_permit);

    let artifact = match persist_graph_index_artifact(db, &catalog, edge_tables_committed).await {
        Ok(artifact) => artifact,
        Err(error) => {
            tracing::warn!(
                target: "omnigraph::optimize",
                error = %error,
                "graph index artifact persist failed; traversals keep building in memory"
            );
            ArtifactRefresh::Failed(error.to_string())
        }
    };

    Ok(OptimizeReport {
        input_commit,
        input_manifest_version,
        graph_commit_id,
        tables,
        resume_at: visited.resume_at.filter(|_| full_catalog),
        gate_hold,
        configuration_clear,
        artifact,
    })
}

/// The tables a run visited, in visit order, each with its staged effect if
/// it has one, and the first table a run-wide bound deferred.
struct VisitedTables {
    tables: Vec<(DatasetOptimizeStats, Option<OptimizeTableEffect>)>,
    resume_at: Option<String>,
}

type PlannedTable = (usize, Instant, Result<OptimizePreparation>);

/// Plan and stage `tasks` in order under the held gates. Plans run up to
/// `concurrency.planning` tables ahead; a table's rewrite starts only after
/// every earlier table's has, with at most `concurrency.rewrite` at once.
/// When a table's turn comes after `deadline`, that table and every later
/// one are deferred and their plans, which only read, are dropped; the first
/// table always starts. A failure fails the run before its publication:
/// `BeforeEffect` until a table's staging starts, `Unpublished` after.
async fn run_table_pipeline(
    db: &Omnigraph,
    catalog: &std::sync::Arc<omnigraph_compiler::catalog::Catalog>,
    tasks: Vec<OptimizeTableTask>,
    concurrency: OptimizeConcurrency,
    deadline: Option<Instant>,
) -> Result<VisitedTables> {
    let keys = tasks
        .iter()
        .map(|task| task.table_key.clone())
        .collect::<Vec<_>>();
    let mut visited: Vec<Option<(DatasetOptimizeStats, Option<OptimizeTableEffect>)>> =
        (0..keys.len()).map(|_| None).collect();
    let plan = |index: usize, task: OptimizeTableTask| {
        let catalog = std::sync::Arc::clone(catalog);
        async move {
            let started = Instant::now();
            let result = prepare_optimize_table(db, catalog.as_ref(), task).await;
            (index, started, result)
        }
    };
    let stage = |index: usize, started: Instant, work: PreparedOptimizeTable| {
        let catalog = std::sync::Arc::clone(catalog);
        async move {
            let result = apply_optimize_table_effects(db, catalog.as_ref(), work).await;
            (index, started, result)
        }
    };

    let mut unplanned = tasks.into_iter().enumerate();
    let mut planning = FuturesOrdered::new();
    let mut ready: VecDeque<PlannedTable> = VecDeque::new();
    let mut staging = FuturesUnordered::new();
    let mut turns = 0usize;
    let mut stopped_at = None;
    let mut staged_any = false;
    let label = |error: OmniError, staged_any: bool| {
        if staged_any {
            error.unpublished()
        } else {
            error.before_effect()
        }
    };

    loop {
        if stopped_at.is_none() {
            while planning.len() + ready.len() < concurrency.planning {
                let Some((index, task)) = unplanned.next() else {
                    break;
                };
                planning.push_back(plan(index, task));
            }
            while staging.len() < concurrency.rewrite {
                let Some((index, started, result)) = ready.pop_front() else {
                    break;
                };
                if turns > 0 && deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    stopped_at = Some(index);
                    break;
                }
                turns += 1;
                match result.map_err(|error| label(error, staged_any))? {
                    OptimizePreparation::Stat(mut stat) => {
                        stat.duration = started.elapsed();
                        visited[index] = Some((stat, None));
                    }
                    OptimizePreparation::Work(work) => {
                        staged_any = true;
                        staging.push(stage(index, started, work));
                    }
                }
            }
            if stopped_at.is_some() {
                planning.clear();
                ready.clear();
            }
        }
        // Filling planning above leaves it non-empty while any table is
        // unplanned, so an empty pipeline has visited every table or stopped.
        if planning.is_empty() && ready.is_empty() && staging.is_empty() {
            break;
        }
        // The deadline stops only a table whose turn has not come.
        let timer = deadline.filter(|_| turns > 0 && turns < keys.len() && stopped_at.is_none());
        tokio::select! {
            biased;
            Some((index, started, result)) = staging.next(), if !staging.is_empty() => {
                let mut outcome = result.map_err(OmniError::unpublished)?;
                outcome.stat.duration = started.elapsed();
                visited[index] = Some((outcome.stat, outcome.effect));
            }
            Some(planned) = planning.next(), if !planning.is_empty() && stopped_at.is_none() => {
                ready.push_back(planned);
            }
            () = tokio::time::sleep_until(timer.unwrap_or_else(Instant::now)), if timer.is_some() => {
                stopped_at = Some(turns);
                planning.clear();
                ready.clear();
            }
        }
    }

    let resume_at = stopped_at.map(|index| keys[index].clone());
    let tables = visited
        .into_iter()
        .zip(keys)
        .map(|(entry, key)| {
            entry.unwrap_or_else(|| {
                (
                    DatasetOptimizeStats::deferred(key, DeferReason::GateHoldLimit),
                    None,
                )
            })
        })
        .collect();
    Ok(VisitedTables { tables, resume_at })
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
/// planned follow-up). The caller holds no writer gate: the build reads one
/// fresh snapshot, and the artifact's stamps name the table versions it read.
async fn persist_graph_index_artifact(
    db: &Omnigraph,
    catalog: &omnigraph_compiler::catalog::Catalog,
    edge_tables_committed: bool,
) -> Result<ArtifactRefresh> {
    if catalog.edge_types.is_empty() {
        return Ok(ArtifactRefresh::Skipped(ArtifactSkip::NoEdgeTypes));
    }
    let uri = crate::graph_index::persist::artifact_uri(db.root_uri());
    if !edge_tables_committed && db.storage_adapter().exists(&uri).await? {
        tracing::debug!(
            target: "omnigraph::optimize",
            "graph index artifact refresh skipped: no edge table advanced and an artifact exists"
        );
        return Ok(ArtifactRefresh::Unchanged);
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
        return Ok(ArtifactRefresh::Skipped(ArtifactSkip::IncompleteEdgeTables));
    }
    let index =
        crate::graph_index::GraphIndex::build(&snapshot, &edge_types, db.catalog().system_columns)
            .await?;
    let written =
        crate::graph_index::persist::save(&snapshot, db.storage_adapter(), &edge_types, &index)
            .await?;
    let Some(uri) = written else {
        return Ok(ArtifactRefresh::Skipped(ArtifactSkip::IncompleteEdgeTables));
    };
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
    Ok(ArtifactRefresh::Refreshed)
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

    // The same plan `stage_compaction` executes: a fragment still holding a
    // dropped property's values is compaction work even when Lance's planner
    // would leave the table alone.
    let options = CompactionOptions::default();
    let will_compact = TableStore::plan_table_compaction(snapshot.dataset(), &options)
        .await?
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
            DatasetOptimizeStats::planned(task.table_key, &CompactionMetrics::default(), false);
        stat.pending_indexes = index_work.pending;
        super::table_ops::append_deferred_full_text_indexes(
            &snapshot,
            &stat.type_key,
            "main",
            &[],
            &mut stat.pending_indexes,
        )
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
    let declared_full_text = super::table_ops::declared_full_text_columns(catalog, &table_key);
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
            .commit_staged_detached(tip, staged, &witness, &declared_full_text)
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
            .commit_staged_detached(tip, staged, &witness, &declared_full_text)
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
            .commit_staged_detached(tip, staged, &witness, &declared_full_text)
            .await?;
        tip = indexed;
        tip_identity = Some(identity);
        fail(&OPTIMIZE_POST_TABLE_EFFECT)?;
    }
    let Some(identity) = tip_identity else {
        let mut stat = DatasetOptimizeStats::planned(table_key, &metrics, false);
        stat.pending_indexes = index_work.pending;
        super::table_ops::append_deferred_full_text_indexes(
            &tip,
            &stat.type_key,
            "main",
            &[],
            &mut stat.pending_indexes,
        )
        .await?;
        return Ok(OptimizeEffectOutcome { stat, effect: None });
    };
    let mut stat = DatasetOptimizeStats::planned(table_key.clone(), &metrics, true);
    stat.pending_indexes = index_work.pending;
    super::table_ops::append_deferred_full_text_indexes(
        &tip,
        &stat.type_key,
        "main",
        &[],
        &mut stat.pending_indexes,
    )
    .await?;
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

const INTERNAL_CONFIG_RETRY_BUDGET: u32 = 5;

fn is_retryable_lance_conflict(err: &lance::Error) -> bool {
    matches!(
        err,
        lance::Error::RetryableCommitConflict { .. }
            | lance::Error::CommitConflict { .. }
            | lance::Error::TooMuchWriteContention { .. }
    )
}

/// Remove automatic GC configuration, returning whether Lance HEAD advanced.
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

decide_seam! {
    /// After the run's publication, before the configuration clear reads
    /// `__manifest`. A failure here must leave the published run reported,
    /// with the clear `failed`.
    pub static OPTIMIZE_CONFIGURATION_CLEAR = ("optimize.configuration_clear", Unreachable, [Fail]);
}

/// Remove stored automatic GC configuration without changing manifest
/// fragments, and report what happened. A failed removal reads `__manifest`
/// again: keys still present are `Failed`, keys gone are `Cleared`, and a
/// read that fails too is `Unknown`. Nothing here fails the run.
async fn clear_manifest_auto_cleanup(db: &Omnigraph) -> ConfigurationClear {
    let uri = crate::db::manifest::manifest_uri(db.root_uri());
    let mut last_error = None;
    for attempt in 0..INTERNAL_CONFIG_RETRY_BUDGET {
        if let Err(error) = fail(&OPTIMIZE_CONFIGURATION_CLEAR) {
            return ConfigurationClear::Failed(error.to_string());
        }
        let mut ds = match db.storage().open_dataset_head(&uri, None).await {
            Ok(handle) => handle.into_dataset(),
            Err(error) => return ConfigurationClear::Failed(error.to_string()),
        };
        match clear_stale_auto_cleanup_config(&mut ds).await {
            Ok(false) => return ConfigurationClear::NothingToClear,
            Ok(true) => {
                if let Err(error) = db.coordinator.write().await.refresh().await {
                    tracing::warn!(
                        target: "omnigraph::optimize",
                        error = %error,
                        "coordinator refresh after the configuration clear failed"
                    );
                }
                return ConfigurationClear::Cleared;
            }
            Err(error)
                if attempt + 1 < INTERNAL_CONFIG_RETRY_BUDGET
                    && is_retryable_lance_conflict(&error) =>
            {
                last_error = Some(error);
                continue;
            }
            Err(error) => {
                last_error = Some(error);
                break;
            }
        }
    }
    let message = last_error.map_or_else(
        || format!("removal exhausted {INTERNAL_CONFIG_RETRY_BUDGET} retries"),
        |error| error.to_string(),
    );
    match db.storage().open_dataset_head(&uri, None).await {
        Ok(handle) => {
            let present = handle
                .dataset()
                .config()
                .keys()
                .any(|key| key.starts_with("lance.auto_cleanup."));
            if present {
                ConfigurationClear::Failed(message)
            } else {
                ConfigurationClear::Cleared
            }
        }
        Err(error) => ConfigurationClear::Unknown(format!("{message}; read-back failed: {error}")),
    }
}

decide_seam! {
    pub static CLEANUP_TABLE_GC = ("cleanup.table_gc", Unreachable, [Fail]);
}

decide_seam! {
    /// After one expired unattached tag's release is confirmed and before
    /// the next one's: the released descendant is gone, its ancestors stay
    /// addressable, and a retried cleanup finishes upward.
    pub static CLEANUP_POST_UNATTACHED_RELEASE = ("cleanup.post_unattached_release", Unreachable, [Fail]);
}

decide_seam! {
    /// After cleanup's entry checks, before it captures authority and
    /// acquires its schema/branch/table GC gate set: the one window where a
    /// failure aborts the whole run (per-table GC failures are isolated).
    pub static CLEANUP_PRE_GATES = ("cleanup.pre_gates", Unreachable, [Fail]);
}

/// Reclaim unretained table versions with the engine collector
/// (`collector.rs`), never stock `cleanup_old_versions`. [`CleanupPolicyOptions`]
/// chooses the `__manifest` versions each live branch retains (every branch
/// head always among them); the collector keeps every file a retained
/// version, tag or merge base reaches and deletes the rest.
pub async fn cleanup_all_datasets(
    db: &Omnigraph,
    options: CleanupPolicyOptions,
    actor: Option<&str>,
) -> Result<Vec<DatasetCleanupStats>> {
    db.enforce(
        omnigraph_policy::PolicyAction::Reclaim,
        &omnigraph_policy::ResourceScope::Graph,
        actor,
    )?;
    if options.keep_versions.is_none() && options.older_than.is_none() {
        return Err(OmniError::manifest(
            "cleanup requires at least one of keep_versions or older_than",
        ));
    }

    let _export_exclusion = db.reserve_export_destructive_control()?;
    db.ensure_schema_state_valid().await?;
    fail(&CLEANUP_PRE_GATES)?;

    // GC must be bound to one accepted graph view. Capture before acquiring
    // writer gates, and revalidate after the complete schema/branch/table
    // envelope before deleting any version history.
    let authority_txn = db.open_write_txn(None).await?;

    let _cleanup_schema_permit = db.write_queue().acquire_schema_shared().await;
    db.refresh_coordinator_only().await?;
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
    let unattached_release = super::collector::unattached_release_order(
        &plan.expired_unattached_tags,
        &plan.retained_unattached_tags,
    );
    if !plan.expired_merge_input_tags.is_empty()
        || !plan.expired_unpublished_tags.is_empty()
        || !unattached_release.is_empty()
    {
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
        for name in &plan.expired_unpublished_tags {
            crate::db::manifest::unpublished::release_unpublished_tag(manifest.dataset(), name)
                .await?;
        }
        let mut unattached_tags_released: u64 = 0;
        for record in &unattached_release {
            crate::db::manifest::unattached::release_unattached_tag_confirmed(
                manifest.dataset(),
                &record.name,
                &record.graph_commit_id,
            )
            .await?;
            unattached_tags_released += 1;
            fail(&CLEANUP_POST_UNATTACHED_RELEASE)?;
        }
        tracing::info!(
            target: "omnigraph::cleanup",
            unattached_tags_released,
            unattached_tags_retained = plan.retained_unattached_tags.len(),
            unattached_tags_blocked = plan.expired_unattached_tags.len() - unattached_release.len(),
            "cleanup released the expired unattached branch tags"
        );
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
    let _schema = db.write_queue().acquire_schema_shared().await;
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

    /// Native commit conflicts allow configuration cleanup to reopen and retry.
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
        let _scenario = crate::seams::FailScenario::setup();
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
            .cleanup_as(
                CleanupPolicyOptions {
                    keep_versions: Some(1),
                    older_than: None,
                },
                None,
            )
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
            .cleanup_as(
                CleanupPolicyOptions {
                    keep_versions: Some(1),
                    older_than: None,
                },
                None,
            )
            .await
            .unwrap();
        assert!(rows.iter().all(|row| row.error.is_none()), "{rows:?}");
        assert!(
            !tree.exists(),
            "complete origin evidence allows orphan reclamation"
        );
    }
}
