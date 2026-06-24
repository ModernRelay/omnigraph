//! Graph-level batch publish over the namespace `__manifest` table.
//!
//! Lance now owns most of the table/version control plane for Omnigraph:
//! table storage, table-local versioning, namespace lookup, and native table
//! history. This module exists for the remaining graph-specific gap:
//! Omnigraph needs one atomic publish point across multiple tables and the
//! current Rust namespace surface does not expose a branch-aware
//! `BatchCreateTableVersions` path for `DirectoryNamespace`.
//!
//! Until Lance exposes that operation directly, this publisher owns only:
//! - validating batch publish invariants against the current `__manifest` state
//! - atomically inserting immutable `table_version` rows into `__manifest`
//! - returning the refreshed manifest dataset that defines the visible graph
//!
//! This module should disappear once Lance Rust can do branch-aware batch table
//! version publication against a managed namespace manifest.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatchIterator;
use async_trait::async_trait;
use lance::Dataset;
use lance::Error as LanceError;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance_namespace::NamespaceError;
#[cfg(test)]
use lance_namespace::models::CreateTableVersionRequest;

use crate::error::{OmniError, Result};

#[cfg(test)]
use super::SubTableUpdate;
use super::layout::{open_manifest_dataset, tombstone_object_id, version_object_id};
use super::metadata::parse_namespace_version_request;
use super::migrations::migrate_internal_schema;
use super::state::{
    GraphLineageRow, GraphLineageRowPart, graph_lineage_row_parts, head_lineage_row,
    manifest_rows_batch, manifest_schema, read_publish_scan,
};
use super::{
    ManifestChange, OBJECT_TYPE_TABLE, OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION,
    SubTableEntry, TableRegistration, TableTombstone,
};

/// Bound on the publisher-level retry loop that wraps Lance's row-level CAS
/// (`TooMuchWriteContention`). Lance's own `conflict_retries` is set to 0 in
/// `merge_rows` because its auto-rebase is "transparent merge" semantics —
/// wrong for an OCC contract — so retry is owned here instead, where each
/// iteration re-runs `load_publish_state` and the expected-version pre-check.
const PUBLISHER_RETRY_BUDGET: u32 = 5;

/// The graph-lineage commit to record atomically with a manifest publish
/// (RFC-013 Phase 7). One logical commit per publish: the `graph_commit_id` is
/// minted once by the caller and stays stable across the publisher's CAS
/// retries; only the parent re-resolves per attempt (against the freshly loaded
/// `__manifest`), so a retry after a concurrent commit parents off the new head
/// — the TOCTOU the dual-write era's `commit_graph.refresh()` guarded is closed
/// by construction.
#[derive(Debug, Clone)]
pub(crate) struct LineageIntent {
    /// ULID minted once before the publish loop; the graph commit's identity.
    pub graph_commit_id: String,
    /// The branch this commit lands on (`None` = main). Selects the
    /// `graph_head:<branch>` pointer row that gets updated.
    pub branch: Option<String>,
    /// Authoring actor, or `None` for unauthored / system writes.
    pub actor_id: Option<String>,
    /// The merged-in source head — `Some` only for a branch-merge commit.
    pub merged_parent_commit_id: Option<String>,
    /// Commit timestamp (microseconds since the UNIX epoch).
    pub created_at: i64,
}

/// The result of a manifest publish that may have folded in a graph commit.
#[derive(Debug)]
pub(super) struct PublishOutcome {
    /// The advanced `__manifest` dataset (its version is the published version).
    pub dataset: Dataset,
    /// The parent the publisher resolved for the recorded commit, if a
    /// [`LineageIntent`] was supplied. Returned so the caller can update its
    /// in-memory commit cache without a re-read. `None` when no lineage was
    /// recorded, or when the commit is the genesis (no parent).
    pub parent_commit_id: Option<String>,
}

#[async_trait]
pub(super) trait ManifestBatchPublisher: Send + Sync {
    async fn publish(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &HashMap<String, u64>,
        lineage: Option<&LineageIntent>,
    ) -> Result<PublishOutcome>;
}

pub(super) struct GraphNamespacePublisher {
    root_uri: String,
    branch: Option<String>,
}

#[derive(Debug)]
struct PendingVersionRow {
    object_id: String,
    object_type: String,
    location: Option<String>,
    metadata: Option<String>,
    table_key: String,
    table_version: Option<u64>,
    table_branch: Option<String>,
    row_count: Option<u64>,
}

/// Everything one CAS attempt needs out of a single `__manifest` scan
/// (RFC-013 P2): the open dataset, table state for the pre-check + pending-row
/// build, and the `graph_commit` lineage rows for parent resolution. Folding the
/// lineage into this struct is what lets `resolve_lineage_rows` skip its own
/// `read_graph_lineage` scan.
struct LoadedPublishState {
    dataset: Dataset,
    registered_tables: HashMap<String, String>,
    existing_versions: HashMap<(String, u64), SubTableEntry>,
    existing_tombstones: HashMap<(String, u64), ()>,
    lineage_rows: Vec<GraphLineageRow>,
}

impl GraphNamespacePublisher {
    pub(super) fn new(root_uri: &str, branch: Option<&str>) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            branch: branch
                .filter(|branch| *branch != "main")
                .map(ToOwned::to_owned),
        }
    }

    async fn dataset(&self) -> Result<Dataset> {
        open_manifest_dataset(&self.root_uri, self.branch.as_deref()).await
    }

    async fn load_publish_state(&self) -> Result<LoadedPublishState> {
        let mut dataset = self.dataset().await?;
        // Run pending internal-schema migrations exactly once per publish on
        // the open-for-write path; idempotent when the on-disk stamp already
        // matches this binary. Pass this publisher's branch so the v3→v4 lineage
        // backfill reads `_graph_commits.lance` at the SAME branch it is
        // publishing to (each branch backfills on its first write). See
        // `db/manifest/migrations.rs`.
        migrate_internal_schema(&mut dataset, &self.root_uri, self.branch.as_deref()).await?;
        // ONE `__manifest` scan for everything the publish needs: table
        // locations, version entries, tombstones, AND the `graph_commit` lineage
        // rows for parent resolution (RFC-013 P2). The lineage extraction rides
        // this pass instead of a second `read_graph_lineage` scan in
        // `resolve_lineage_rows`; the per-attempt re-read is preserved because
        // `load_publish_state` runs once per CAS attempt, so a retry sees the
        // advanced head and re-parents correctly.
        let scan = read_publish_scan(&dataset).await?;
        let existing_versions = scan
            .version_entries
            .iter()
            .map(|entry| {
                (
                    (entry.table_key.clone(), entry.table_version),
                    entry.clone(),
                )
            })
            .collect();
        let existing_tombstones = scan.tombstones.into_iter().collect();
        Ok(LoadedPublishState {
            dataset,
            registered_tables: scan.table_locations,
            existing_versions,
            existing_tombstones,
            lineage_rows: scan.lineage_rows,
        })
    }

    fn build_pending_rows(
        changes: &[ManifestChange],
        known_tables: &HashMap<String, String>,
        existing_versions: &HashMap<(String, u64), SubTableEntry>,
        existing_tombstones: &HashMap<(String, u64), ()>,
    ) -> Result<Vec<PendingVersionRow>> {
        let mut request_versions = HashMap::<(String, u64), ()>::new();
        let mut known_tables = known_tables.clone();
        let mut rows = Vec::with_capacity(changes.len());

        for change in changes {
            if let ManifestChange::RegisterTable(TableRegistration {
                table_key,
                table_path,
            }) = change
            {
                if let Some(existing_path) = known_tables.get(table_key) {
                    if existing_path != table_path {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table {} already exists with different path {}",
                                    table_key, existing_path
                                ),
                            }
                            .to_string(),
                        ));
                    }
                } else {
                    known_tables.insert(table_key.clone(), table_path.clone());
                }
                rows.push(PendingVersionRow {
                    object_id: table_key.clone(),
                    object_type: OBJECT_TYPE_TABLE.to_string(),
                    location: Some(table_path.clone()),
                    metadata: None,
                    table_key: table_key.clone(),
                    table_version: None,
                    table_branch: None,
                    row_count: None,
                });
            }
        }

        for change in changes {
            match change {
                ManifestChange::RegisterTable(_) => {}
                ManifestChange::Update(update) => {
                    let request = update.to_create_table_version_request();
                    let (table_key, table_version, row_count, table_branch, version_metadata) =
                        parse_namespace_version_request(&request)
                            .map_err(|e| OmniError::Lance(e.to_string()))?;
                    if !known_tables.contains_key(table_key.as_str()) {
                        return Err(OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!("table {} not found", table_key),
                            }
                            .to_string(),
                        ));
                    }
                    if request_versions
                        .insert((table_key.clone(), table_version), ())
                        .is_some()
                    {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table version {} already exists for {}",
                                    table_version, table_key
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if let Some(existing) =
                        existing_versions.get(&(table_key.clone(), table_version))
                    {
                        let is_owner_branch_handoff = existing.row_count == row_count
                            && existing.table_branch != table_branch;
                        if !is_owner_branch_handoff {
                            return Err(OmniError::Lance(
                                NamespaceError::ConcurrentModification {
                                    message: format!(
                                        "table version {} already exists for {}",
                                        table_version, table_key
                                    ),
                                }
                                .to_string(),
                            ));
                        }
                    }

                    rows.push(PendingVersionRow {
                        object_id: version_object_id(&table_key, table_version),
                        object_type: OBJECT_TYPE_TABLE_VERSION.to_string(),
                        location: None,
                        metadata: Some(version_metadata.to_json_string()?),
                        table_key,
                        table_version: Some(table_version),
                        table_branch,
                        row_count: Some(row_count),
                    });
                }
                ManifestChange::Tombstone(TableTombstone {
                    table_key,
                    tombstone_version,
                }) => {
                    if !known_tables.contains_key(table_key.as_str()) {
                        return Err(OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!("table {} not found", table_key),
                            }
                            .to_string(),
                        ));
                    }
                    if existing_tombstones.contains_key(&(table_key.clone(), *tombstone_version)) {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table tombstone {} already exists for {}",
                                    tombstone_version, table_key
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    rows.push(PendingVersionRow {
                        object_id: tombstone_object_id(table_key, *tombstone_version),
                        object_type: OBJECT_TYPE_TABLE_TOMBSTONE.to_string(),
                        location: None,
                        metadata: None,
                        table_key: table_key.clone(),
                        table_version: Some(*tombstone_version),
                        table_branch: None,
                        row_count: None,
                    });
                }
            }
        }

        Ok(rows)
    }

    /// Resolve the parent for `intent` against the just-loaded `dataset` and
    /// build the two lineage rows (`graph_commit` + `graph_head:<branch>`) to
    /// fold into the publish batch. Runs INSIDE the CAS retry loop, so the
    /// parent is read from the manifest state this attempt will commit against —
    /// a retry after a concurrent commit re-reads the advanced head and parents
    /// correctly (TOCTOU closed). `new_manifest_version` is the version this
    /// publish produces (the recorded commit pins it).
    ///
    /// The parent is the current head of the branch's lineage — the
    /// `should_replace_head` winner over the visible `graph_commit` rows, the
    /// same selection the commit-graph cache uses. (The denormalized
    /// `graph_head:<branch>` row is written for forward-compat but is not the
    /// parent source here: a branch freshly forked from main inherits main's
    /// commits but not yet a `graph_head:<its-name>` row, and the head-over-rows
    /// computation gives the correct fork-point parent in that case.)
    ///
    /// `lineage_rows` is the `graph_commit` set this attempt already parsed in
    /// `load_publish_state`'s single scan (RFC-013 P2) — NOT a fresh
    /// `read_graph_lineage` scan. The per-attempt re-read is still preserved: the
    /// retry loop re-runs `load_publish_state`, so each attempt's `lineage_rows`
    /// reflects the head as it stands for that attempt.
    fn resolve_lineage_rows(
        lineage_rows: &[GraphLineageRow],
        intent: &LineageIntent,
        new_manifest_version: u64,
    ) -> Result<(Vec<PendingVersionRow>, Option<String>)> {
        let parent_commit_id = head_lineage_row(lineage_rows).map(|h| h.graph_commit_id.clone());

        let commit = GraphLineageRow {
            graph_commit_id: intent.graph_commit_id.clone(),
            manifest_branch: intent.branch.clone(),
            manifest_version: new_manifest_version,
            parent_commit_id: parent_commit_id.clone(),
            merged_parent_commit_id: intent.merged_parent_commit_id.clone(),
            actor_id: intent.actor_id.clone(),
            created_at: intent.created_at,
        };
        let parts = graph_lineage_row_parts(&commit, intent.branch.as_deref())?;
        Ok((
            parts.into_iter().map(lineage_part_to_pending).collect(),
            parent_commit_id,
        ))
    }

    fn pending_rows_to_batch(rows: Vec<PendingVersionRow>) -> Result<arrow_array::RecordBatch> {
        let mut object_ids = Vec::with_capacity(rows.len());
        let mut object_types = Vec::with_capacity(rows.len());
        let mut locations: Vec<Option<String>> = Vec::with_capacity(rows.len());
        let mut metadata = Vec::with_capacity(rows.len());
        let mut table_keys = Vec::with_capacity(rows.len());
        let mut table_versions: Vec<Option<u64>> = Vec::with_capacity(rows.len());
        let mut table_branches = Vec::with_capacity(rows.len());
        let mut row_counts: Vec<Option<u64>> = Vec::with_capacity(rows.len());

        for row in rows {
            object_ids.push(row.object_id);
            object_types.push(row.object_type);
            locations.push(row.location);
            metadata.push(row.metadata);
            table_keys.push(row.table_key);
            table_versions.push(row.table_version);
            table_branches.push(row.table_branch);
            row_counts.push(row.row_count);
        }

        manifest_rows_batch(
            object_ids,
            object_types,
            locations,
            metadata,
            table_keys,
            table_versions,
            table_branches,
            row_counts,
        )
    }

    /// Reduce the loaded `(table_key, table_version) → entry` map and the
    /// tombstone set to "latest non-tombstoned version per table" — the same
    /// reduction performed by `read_manifest_state` on the visible snapshot.
    /// Tombstoned tables fall back to their highest tombstone version so that
    /// the resulting `actual` reported in `ExpectedVersionMismatch` is
    /// meaningful even when the caller's expected table no longer exists.
    fn latest_visible_per_table(
        existing_versions: &HashMap<(String, u64), SubTableEntry>,
        existing_tombstones: &HashMap<(String, u64), ()>,
    ) -> HashMap<String, u64> {
        let mut max_tombstones = HashMap::<String, u64>::new();
        for (key, version) in existing_tombstones.keys() {
            max_tombstones
                .entry(key.clone())
                .and_modify(|v| {
                    if *version > *v {
                        *v = *version;
                    }
                })
                .or_insert(*version);
        }

        let mut latest = HashMap::<String, u64>::new();
        for (key, version) in existing_versions.keys() {
            let tombstoned = max_tombstones
                .get(key)
                .map(|t| *t >= *version)
                .unwrap_or(false);
            if tombstoned {
                continue;
            }
            latest
                .entry(key.clone())
                .and_modify(|v| {
                    if *version > *v {
                        *v = *version;
                    }
                })
                .or_insert(*version);
        }

        // For tables that have only tombstones (no visible entry), surface the
        // tombstone version so callers see a non-zero `actual`.
        for (key, tombstone) in &max_tombstones {
            latest.entry(key.clone()).or_insert(*tombstone);
        }

        latest
    }

    /// Compare each caller-supplied expectation against the manifest's current
    /// latest visible version per table. The first mismatch is returned as a
    /// typed `ExpectedVersionMismatch` (`actual = 0` if the table isn't in the
    /// manifest at all).
    fn check_expected_table_versions(
        latest_per_table: &HashMap<String, u64>,
        expected: &HashMap<String, u64>,
    ) -> Result<()> {
        for (table_key, expected_version) in expected {
            let actual = latest_per_table.get(table_key).copied().unwrap_or(0);
            if actual != *expected_version {
                return Err(OmniError::manifest_expected_version_mismatch(
                    table_key.clone(),
                    *expected_version,
                    actual,
                ));
            }
        }
        Ok(())
    }

    async fn merge_rows(&self, dataset: Dataset, rows: Vec<PendingVersionRow>) -> Result<Dataset> {
        let batch = Self::pending_rows_to_batch(rows)?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], manifest_schema());
        let dataset = Arc::new(dataset);
        let mut merge_builder = MergeInsertBuilder::try_new(dataset, vec!["object_id".to_string()])
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        merge_builder.when_matched(WhenMatched::UpdateAll);
        merge_builder.when_not_matched(WhenNotMatched::InsertAll);
        // 0 here is intentional: Lance's built-in retry uses transparent rebase,
        // which would let a concurrent writer's row land alongside ours and
        // silently break the OCC contract on `__manifest`. Retries are owned by
        // the publisher loop above, where each attempt re-runs the pre-check.
        merge_builder.conflict_retries(0);
        merge_builder.use_index(false);
        // Skip Lance's auto-cleanup hook: `__manifest` versions are the snapshot
        // / time-travel authority and must never be GC'd by Lance's per-commit
        // hook. A `__manifest` created before the v7 bump (6.0.1 defaulted
        // auto_cleanup ON) still carries the stored config, so this skip is
        // load-bearing on upgraded graphs, not just defensive.
        merge_builder.skip_auto_cleanup(true);
        let (new_dataset, _stats) = merge_builder
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .execute_reader(Box::new(reader))
            .await
            .map_err(map_lance_publish_error)?;
        Ok(Arc::try_unwrap(new_dataset).unwrap_or_else(|arc| (*arc).clone()))
    }

    #[cfg(test)]
    pub(super) async fn publish_requests(
        &self,
        requests: &[CreateTableVersionRequest],
    ) -> Result<Dataset> {
        let changes = requests
            .iter()
            .cloned()
            .map(|request| {
                let (table_key, table_version, row_count, table_branch, version_metadata) =
                    parse_namespace_version_request(&request)
                        .map_err(|e| OmniError::Lance(e.to_string()))?;
                Ok(ManifestChange::Update(SubTableUpdate {
                    table_key,
                    table_version,
                    table_branch,
                    row_count,
                    version_metadata,
                }))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.publish(&changes, &HashMap::new(), None).await?.dataset)
    }
}

/// Map a `state::GraphLineageRowPart` onto a `PendingVersionRow` so a graph
/// commit's two lineage rows ride the same publish batch as the table-version
/// rows (RFC-013 Phase 7). Lineage rows carry no table identity: `table_key` is
/// the empty string (never matched by a real key) and `location`/`row_count`
/// are null.
fn lineage_part_to_pending(part: GraphLineageRowPart) -> PendingVersionRow {
    PendingVersionRow {
        object_id: part.object_id,
        object_type: part.object_type.to_string(),
        location: None,
        metadata: Some(part.metadata),
        table_key: String::new(),
        table_version: part.table_version,
        table_branch: part.table_branch,
        row_count: None,
    }
}

/// `Error::TooMuchWriteContention` from Lance's row-level CAS bubbles up here
/// when a concurrent writer landed a row with the same `object_id` (the
/// merge-insert join key, annotated as an unenforced primary key on
/// `__manifest`). Translate it to a typed manifest conflict so callers can
/// match without parsing strings; everything else is opaque storage.
///
/// Shared (`pub(crate)`) with the v3→v4 lineage backfill
/// (`state::merge_lineage_rows`), which issues its own `__manifest` merge-insert
/// outside the publisher and must surface the SAME typed
/// `RowLevelCasContention` so the migration's re-open retry loop can recognize a
/// CAS loss. This is the merge-insert (`execute_reader`) conflict vocabulary
/// only. It is deliberately NOT `optimize::is_retryable_lance_conflict`: that one
/// also matches `CommitConflict`/`RetryableCommitConflict` from the COMPACTION
/// commit path (`compact_files` -> `apply_commit`), which a row-level merge-insert
/// never emits — folding it in here would match impossible variants.
pub(crate) fn map_lance_publish_error(err: LanceError) -> OmniError {
    if matches!(err, LanceError::TooMuchWriteContention { .. }) {
        return OmniError::manifest_row_level_cas_contention(format!(
            "manifest publish lost a row-level CAS race: {}",
            err
        ));
    }
    OmniError::Lance(err.to_string())
}

#[async_trait]
impl ManifestBatchPublisher for GraphNamespacePublisher {
    async fn publish(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &HashMap<String, u64>,
        lineage: Option<&LineageIntent>,
    ) -> Result<PublishOutcome> {
        if changes.is_empty() && expected_table_versions.is_empty() && lineage.is_none() {
            return Ok(PublishOutcome {
                dataset: self.dataset().await?,
                parent_commit_id: None,
            });
        }

        for attempt in 0..=PUBLISHER_RETRY_BUDGET {
            let LoadedPublishState {
                dataset,
                registered_tables: known_tables,
                existing_versions,
                existing_tombstones,
                lineage_rows,
            } = self.load_publish_state().await?;

            let latest_per_table =
                Self::latest_visible_per_table(&existing_versions, &existing_tombstones);
            // Pre-check on every attempt against freshly loaded state so a
            // concurrent commit that broke the caller's expectation is
            // surfaced as `ExpectedVersionMismatch` rather than retried.
            Self::check_expected_table_versions(&latest_per_table, expected_table_versions)?;

            let mut rows = Self::build_pending_rows(
                changes,
                &known_tables,
                &existing_versions,
                &existing_tombstones,
            )?;

            // Fold the graph commit into the SAME batch so table-version rows
            // and lineage rows land in one merge-insert (one Lance commit, one
            // manifest version) — no separate write, no manifest→commit-graph
            // atomicity gap. The merge-insert advances exactly one version on
            // top of the loaded dataset, so the commit pins
            // `current + 1`. The parent is resolved here, per attempt, from the
            // lineage rows THIS attempt's scan loaded (TOCTOU closed on a CAS
            // retry — a retry re-runs `load_publish_state` → fresh lineage).
            let parent_commit_id = match lineage {
                Some(intent) => {
                    let new_manifest_version = dataset.version().version + 1;
                    let (commit_rows, parent) =
                        Self::resolve_lineage_rows(&lineage_rows, intent, new_manifest_version)?;
                    rows.extend(commit_rows);
                    parent
                }
                None => None,
            };

            if rows.is_empty() {
                // Expected-version-only publish with no changes and no lineage:
                // the precondition held, nothing to write.
                return Ok(PublishOutcome {
                    dataset,
                    parent_commit_id,
                });
            }

            match self.merge_rows(dataset, rows).await {
                Ok(new_dataset) => {
                    return Ok(PublishOutcome {
                        dataset: new_dataset,
                        parent_commit_id,
                    });
                }
                Err(err) => {
                    if attempt < PUBLISHER_RETRY_BUDGET && is_retryable_publish_conflict(&err) {
                        continue;
                    }
                    return Err(err);
                }
            }
        }

        Err(OmniError::manifest_conflict(format!(
            "manifest publish exhausted {} retries against concurrent writers",
            PUBLISHER_RETRY_BUDGET
        )))
    }
}

/// A retryable conflict here means: Lance's row-level CAS rejected our commit
/// because someone else landed an `object_id` we were also inserting (mapped
/// from `Error::TooMuchWriteContention` to
/// `ManifestConflictDetails::RowLevelCasContention`). This is transparent
/// contention; if the caller's `expected_table_versions` still holds against
/// the new manifest state, we re-attempt. Other conflict variants (notably
/// `ExpectedVersionMismatch`) propagate so the caller learns immediately.
///
/// Shared (`pub(crate)`) with the v3→v4 lineage backfill's re-open retry loop
/// (`migrations::migrate_v3_to_v4`), so the migration's retry decision matches the
/// publisher's by construction — both retry exactly `RowLevelCasContention` and
/// propagate everything else.
pub(crate) fn is_retryable_publish_conflict(err: &OmniError) -> bool {
    matches!(
        err,
        OmniError::Manifest(m)
            if matches!(
                m.details,
                Some(crate::error::ManifestConflictDetails::RowLevelCasContention)
            )
    )
}
