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
use super::layout::{
    open_manifest_dataset_with_session, stream_state_object_id, table_object_id,
    tombstone_object_id, version_object_id,
};
use super::metadata::{TableVersionMetadata, parse_namespace_version_request};
use super::migrations::{read_stamp, refuse_if_stamp_unsupported};
use super::state::{
    GraphLineageRow, GraphLineageRowPart, ManifestState, assemble_manifest_state,
    graph_head_object_id, graph_lineage_row_parts, head_lineage_row, manifest_rows_batch,
    manifest_schema, read_manifest_state, read_publish_scan,
};
use super::{
    ExpectedTableVersions, MAIN_BRANCH_HEAD_KEY, ManifestChange, OBJECT_TYPE_STREAM_STATE,
    OBJECT_TYPE_TABLE, OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION,
    StreamLifecycleEntry, SubTableEntry, TableIdentity, TableRegistration, TableRename,
    TableTombstone,
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
/// retries. Legacy [`PublishPrecondition::Any`] publishes re-resolve the parent
/// per attempt. An exact-head publish instead rejects a retry once that authority
/// changed, so a prepared write can never be silently re-parented.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

/// The exact mutable graph-head authority a prepared write observed. A missing
/// row is first-class: a freshly-created named branch inherits lineage commits
/// but has no `graph_head:<its-name>` until its first graph commit.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct GraphHeadExpectation {
    /// `None` means main; `Some("main")` is normalized to `None` by [`new`].
    pub(crate) branch: Option<String>,
    /// Lance-native stable branch identity. This detects delete/recreate ABA;
    /// manifest versions/eTags are deliberately not branch identity.
    pub(crate) branch_identifier: lance::dataset::refs::BranchIdentifier,
    /// Exact commit id stored in the branch's head row, or `None` when absent.
    pub(crate) head_commit_id: Option<String>,
}

impl GraphHeadExpectation {
    pub(crate) fn new(
        branch: Option<&str>,
        branch_identifier: lance::dataset::refs::BranchIdentifier,
        head_commit_id: Option<String>,
    ) -> Self {
        Self {
            branch: branch
                .filter(|branch| *branch != "main")
                .map(ToOwned::to_owned),
            branch_identifier,
            head_commit_id,
        }
    }

    fn object_id(&self) -> String {
        graph_head_object_id(self.branch.as_deref())
    }
}

/// Authority checked by the manifest publisher on every CAS attempt.
///
/// `Any` preserves the legacy dispatcher semantics: row-level contention may
/// retry and re-parent a lineage intent. `ExactGraphHead` is the RFC-022
/// foundation for prepared writes: after contention, any head movement becomes
/// `ReadSetChanged` rather than a transparent re-parent. Its native branch-id
/// check detects delete/recreate ABA on every attempt; it is not a distributed
/// ref-control fence (Lance branch create/delete still lacks conditional CAS),
/// so branch control remains within the documented single-writer-process bound.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum PublishPrecondition {
    Any,
    ExactGraphHead(GraphHeadExpectation),
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
    /// The new visible per-table state, folded in-memory from the pre-publish
    /// state ∪ the committed rows (RFC-013 PR2 #1b). Returned so the caller skips
    /// the O(fragments) post-publish `read_manifest_state` re-scan. Byte-identical
    /// to that re-scan: built through the same `assemble_manifest_state` reduction.
    pub known_state: ManifestState,
}

#[async_trait]
pub(super) trait ManifestBatchPublisher: Send + Sync {
    /// Compatibility/default publish behavior for bounded or recovery paths
    /// that do not carry an exact graph-head precondition. Exact RFC-022
    /// adapters call `publish_with_precondition` directly.
    async fn publish(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
    ) -> Result<PublishOutcome> {
        self.publish_with_precondition(
            changes,
            expected_table_versions,
            lineage,
            &PublishPrecondition::Any,
        )
        .await
    }

    async fn publish_with_precondition(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<PublishOutcome>;
}

pub(super) struct GraphNamespacePublisher {
    root_uri: String,
    branch: Option<String>,
    control_session: Arc<lance::session::Session>,
}

#[derive(Debug)]
struct PendingVersionRow {
    object_id: String,
    object_type: String,
    location: Option<String>,
    metadata: Option<String>,
    table_key: String,
    identity: Option<TableIdentity>,
    table_version: Option<u64>,
    table_branch: Option<String>,
    row_count: Option<u64>,
}

/// Everything one CAS attempt needs out of a single `__manifest` scan
/// (RFC-013 P2): the open dataset, table state for the pre-check + pending-row
/// build, `graph_commit` lineage rows for parent resolution, and exact
/// `graph_head` rows for OCC. Folding lineage authority into this struct is what
/// lets both checks skip their own `read_graph_lineage` scan.
struct LoadedPublishState {
    dataset: Dataset,
    registered_tables: HashMap<TableIdentity, TableRegistration>,
    existing_versions: HashMap<(TableIdentity, u64), SubTableEntry>,
    existing_tombstones: HashMap<(TableIdentity, u64), ()>,
    lineage_rows: Vec<GraphLineageRow>,
    graph_heads: HashMap<String, String>,
    stream_lifecycles: HashMap<TableIdentity, StreamLifecycleEntry>,
}

impl GraphNamespacePublisher {
    #[cfg(test)]
    pub(super) fn new(root_uri: &str, branch: Option<&str>) -> Self {
        Self::new_with_session(root_uri, branch, crate::lance_access::control_session())
    }

    pub(super) fn new_with_session(
        root_uri: &str,
        branch: Option<&str>,
        control_session: Arc<lance::session::Session>,
    ) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            branch: branch
                .filter(|branch| *branch != "main")
                .map(ToOwned::to_owned),
            control_session,
        }
    }

    async fn dataset(&self) -> Result<Dataset> {
        open_manifest_dataset_with_session(
            &self.root_uri,
            self.branch.as_deref(),
            &self.control_session,
        )
        .await
    }

    async fn load_publish_state(&self) -> Result<LoadedPublishState> {
        // Test seam: inject a retryable contention here to exercise the outer
        // retry loop's re-run-on-retryable-load-error path (no-op without the
        // `failpoints` feature). The migration surfaces the same typed error.
        crate::failpoints::maybe_fail_retryable_contention(
            crate::failpoints::names::PUBLISH_LOAD_STATE_RETRYABLE_CONTENTION,
        )?;
        let dataset = self.dataset().await?;
        // Refuse a graph this binary cannot serve before publishing. Fresh and
        // already-current graphs pass; a sub-CURRENT stamp (an older storage
        // format) is refused with the rebuild-via-export/import message. There is
        // no in-place migration — storage-format changes are a cutover. See
        // `db/manifest/migrations.rs`.
        refuse_if_stamp_unsupported(read_stamp(&dataset))?;
        // ONE `__manifest` scan for everything the publish needs: table
        // locations, version entries, tombstones, `graph_commit` lineage rows
        // for parent resolution, AND exact `graph_head` rows for OCC (RFC-013
        // P2 / RFC-022). Extraction rides this pass instead of a second
        // `read_graph_lineage` scan; the per-attempt re-read is preserved because
        // `load_publish_state` runs once per CAS attempt.
        let scan = read_publish_scan(&dataset).await?;
        let existing_versions = scan
            .version_entries
            .iter()
            .map(|entry| ((entry.identity, entry.table_version), entry.clone()))
            .collect();
        let existing_tombstones = scan.tombstones.into_iter().collect();
        Ok(LoadedPublishState {
            dataset,
            registered_tables: scan.table_registrations,
            existing_versions,
            existing_tombstones,
            lineage_rows: scan.lineage_rows,
            graph_heads: scan.graph_heads,
            stream_lifecycles: scan.stream_lifecycles,
        })
    }

    fn build_pending_rows(
        changes: &[ManifestChange],
        known_tables: &HashMap<TableIdentity, TableRegistration>,
        existing_versions: &HashMap<(TableIdentity, u64), SubTableEntry>,
        existing_tombstones: &HashMap<(TableIdentity, u64), ()>,
        existing_stream_lifecycles: &HashMap<TableIdentity, StreamLifecycleEntry>,
    ) -> Result<Vec<PendingVersionRow>> {
        let mut request_versions = HashMap::<(TableIdentity, u64), ()>::new();
        let mut binding_changes = HashMap::<TableIdentity, ()>::new();
        let mut known_tables = known_tables.clone();
        let mut rows = Vec::with_capacity(changes.len());
        let mut lifecycle_changes = HashMap::<TableIdentity, ()>::new();
        let mut max_tombstones = HashMap::<TableIdentity, u64>::new();
        for (identity, version) in existing_tombstones.keys() {
            max_tombstones
                .entry(*identity)
                .and_modify(|current| *current = (*current).max(*version))
                .or_insert(*version);
        }
        for change in changes {
            if let ManifestChange::Tombstone(tombstone) = change {
                max_tombstones
                    .entry(tombstone.identity)
                    .and_modify(|current| *current = (*current).max(tombstone.tombstone_version))
                    .or_insert(tombstone.tombstone_version);
            }
        }
        let mut effective_pointers = HashMap::<TableIdentity, (u64, Option<String>)>::new();
        for entry in existing_versions.values() {
            if max_tombstones
                .get(&entry.identity)
                .is_some_and(|tombstone| *tombstone >= entry.table_version)
            {
                continue;
            }
            effective_pointers
                .entry(entry.identity)
                .and_modify(|current| {
                    if entry.table_version > current.0 {
                        *current = (entry.table_version, entry.table_branch.clone());
                    }
                })
                .or_insert_with(|| (entry.table_version, entry.table_branch.clone()));
        }
        for change in changes {
            if let ManifestChange::Update(update) = change {
                if max_tombstones
                    .get(&update.identity)
                    .is_some_and(|tombstone| *tombstone >= update.table_version)
                {
                    continue;
                }
                effective_pointers
                    .entry(update.identity)
                    .and_modify(|current| {
                        if update.table_version >= current.0 {
                            *current = (update.table_version, update.table_branch.clone());
                        }
                    })
                    .or_insert_with(|| (update.table_version, update.table_branch.clone()));
            }
        }

        // Registration and rename rows are applied first so an update in the
        // same batch resolves through the post-change binding.
        for change in changes {
            match change {
                ManifestChange::RegisterTable(registration) => {
                    registration.identity.validate()?;
                    let canonical_path = super::table_path_for_identity(
                        &registration.table_key,
                        registration.identity,
                    )?;
                    if canonical_path != registration.table_path {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table {} identity {} must use canonical path {}, got {}",
                                    registration.table_key,
                                    registration.identity,
                                    canonical_path,
                                    registration.table_path,
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if let Some(existing) = known_tables.get(&registration.identity) {
                        if existing == registration {
                            continue;
                        }
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table identity {} is already registered as {} at {}",
                                    registration.identity, existing.table_key, existing.table_path,
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if binding_changes.insert(registration.identity, ()).is_some() {
                        return Err(OmniError::manifest(format!(
                            "manifest batch changes table binding {} more than once",
                            registration.identity
                        )));
                    }
                    known_tables.insert(registration.identity, registration.clone());
                    rows.push(PendingVersionRow {
                        object_id: table_object_id(registration.identity),
                        object_type: OBJECT_TYPE_TABLE.to_string(),
                        location: Some(registration.table_path.clone()),
                        metadata: None,
                        table_key: registration.table_key.clone(),
                        identity: Some(registration.identity),
                        table_version: None,
                        table_branch: None,
                        row_count: None,
                    });
                }
                ManifestChange::RenameTable(TableRename {
                    identity,
                    expected_table_key,
                    table_key,
                    table_path,
                }) => {
                    identity.validate()?;
                    if binding_changes.insert(*identity, ()).is_some() {
                        return Err(OmniError::manifest(format!(
                            "manifest batch changes table binding {identity} more than once"
                        )));
                    }
                    let existing = known_tables.get(identity).ok_or_else(|| {
                        OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!("table identity {identity} not found"),
                            }
                            .to_string(),
                        )
                    })?;
                    if !Self::is_live_identity(*identity, existing_versions, existing_tombstones) {
                        return Err(OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!(
                                    "live table identity {identity} not found for rename"
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if existing.table_key != *expected_table_key
                        || existing.table_path != *table_path
                    {
                        return Err(OmniError::manifest_read_set_changed(
                            format!("table_binding:{identity}"),
                            Some(format!("{expected_table_key}@{table_path}")),
                            Some(format!("{}@{}", existing.table_key, existing.table_path)),
                        ));
                    }
                    let canonical_path = super::table_path_for_identity(table_key, *identity)?;
                    if canonical_path != *table_path {
                        return Err(OmniError::manifest(format!(
                            "rename of table identity {identity} must preserve physical path \
                             {table_path}; alias '{table_key}' implies {canonical_path}"
                        )));
                    }
                    if table_key == expected_table_key {
                        continue;
                    }
                    let renamed = TableRegistration {
                        identity: *identity,
                        table_key: table_key.clone(),
                        table_path: table_path.clone(),
                    };
                    known_tables.insert(*identity, renamed);
                    rows.push(PendingVersionRow {
                        object_id: table_object_id(*identity),
                        object_type: OBJECT_TYPE_TABLE.to_string(),
                        location: Some(table_path.clone()),
                        metadata: None,
                        table_key: table_key.clone(),
                        identity: Some(*identity),
                        table_version: None,
                        table_branch: None,
                        row_count: None,
                    });
                }
                ManifestChange::Update(_)
                | ManifestChange::Tombstone(_)
                | ManifestChange::SetStreamLifecycle { .. } => {}
            }
        }

        for change in changes {
            match change {
                ManifestChange::RegisterTable(_) | ManifestChange::RenameTable(_) => {}
                ManifestChange::Update(update) => {
                    update.identity.validate()?;
                    let request = update.to_create_table_version_request();
                    let (table_key, table_version, row_count, table_branch, version_metadata) =
                        parse_namespace_version_request(&request)
                            .map_err(|e| OmniError::Lance(e.to_string()))?;
                    let registration = known_tables.get(&update.identity).ok_or_else(|| {
                        OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!("table identity {} not found", update.identity),
                            }
                            .to_string(),
                        )
                    })?;
                    if registration.table_key != table_key {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table identity {} is bound to {}, not {}",
                                    update.identity, registration.table_key, table_key
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if request_versions
                        .insert((update.identity, table_version), ())
                        .is_some()
                    {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table version {} already exists for identity {} ({})",
                                    table_version, update.identity, table_key
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if let Some(existing) = existing_versions.get(&(update.identity, table_version))
                    {
                        let is_owner_branch_handoff = existing.row_count == row_count
                            && existing.table_branch != table_branch;
                        if !is_owner_branch_handoff {
                            return Err(OmniError::Lance(
                                NamespaceError::ConcurrentModification {
                                    message: format!(
                                        "table version {} already exists for identity {} ({})",
                                        table_version, update.identity, table_key
                                    ),
                                }
                                .to_string(),
                            ));
                        }
                    }

                    rows.push(PendingVersionRow {
                        object_id: version_object_id(update.identity, table_version),
                        object_type: OBJECT_TYPE_TABLE_VERSION.to_string(),
                        location: None,
                        metadata: Some(version_metadata.to_json_string()?),
                        table_key,
                        identity: Some(update.identity),
                        table_version: Some(table_version),
                        table_branch,
                        row_count: Some(row_count),
                    });
                }
                ManifestChange::Tombstone(TableTombstone {
                    identity,
                    table_key,
                    tombstone_version,
                }) => {
                    identity.validate()?;
                    let registration = known_tables.get(identity).ok_or_else(|| {
                        OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!("table identity {identity} not found"),
                            }
                            .to_string(),
                        )
                    })?;
                    if registration.table_key != *table_key {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table identity {identity} is bound to {}, not {}",
                                    registration.table_key, table_key
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    if existing_tombstones.contains_key(&(*identity, *tombstone_version)) {
                        return Err(OmniError::Lance(
                            NamespaceError::ConcurrentModification {
                                message: format!(
                                    "table tombstone {} already exists for identity {} ({})",
                                    tombstone_version, identity, table_key
                                ),
                            }
                            .to_string(),
                        ));
                    }
                    rows.push(PendingVersionRow {
                        object_id: tombstone_object_id(*identity, *tombstone_version),
                        object_type: OBJECT_TYPE_TABLE_TOMBSTONE.to_string(),
                        location: None,
                        metadata: None,
                        table_key: table_key.clone(),
                        identity: Some(*identity),
                        table_version: Some(*tombstone_version),
                        table_branch: None,
                        row_count: None,
                    });
                }
                ManifestChange::SetStreamLifecycle { expected, next } => {
                    if let Some(expected) = expected {
                        expected.validate()?;
                        if expected.identity != next.identity {
                            return Err(OmniError::manifest_internal(format!(
                                "stream lifecycle expected identity {} does not match next identity {}",
                                expected.identity, next.identity
                            )));
                        }
                    }
                    let current = existing_stream_lifecycles.get(&next.identity);
                    if current != expected.as_ref() {
                        return Err(OmniError::manifest_read_set_changed(
                            stream_state_object_id(next.identity),
                            expected
                                .as_ref()
                                .map(StreamLifecycleEntry::to_metadata_json)
                                .transpose()?,
                            current
                                .map(StreamLifecycleEntry::to_metadata_json)
                                .transpose()?,
                        ));
                    }
                    let lifecycle = next;
                    lifecycle.validate()?;
                    let effective_pointer = effective_pointers.get(&lifecycle.identity).ok_or_else(
                        || {
                            OmniError::manifest_internal(format!(
                                "stream lifecycle {} has no live effective table pointer in this batch",
                                lifecycle.identity
                            ))
                        },
                    )?;
                    if lifecycle.current_head_witness.table_version != effective_pointer.0
                        || lifecycle.binding.table_branch != effective_pointer.1
                    {
                        return Err(OmniError::manifest(format!(
                            "stream lifecycle {} witnesses {:?} version {}, but this batch's effective table pointer is {:?} version {}",
                            lifecycle.identity,
                            lifecycle.binding.table_branch,
                            lifecycle.current_head_witness.table_version,
                            effective_pointer.1,
                            effective_pointer.0,
                        )));
                    }
                    let registration = known_tables.get(&lifecycle.identity).ok_or_else(|| {
                        OmniError::Lance(
                            NamespaceError::TableNotFound {
                                message: format!(
                                    "table identity {} not found for stream lifecycle",
                                    lifecycle.identity
                                ),
                            }
                            .to_string(),
                        )
                    })?;
                    lifecycle.validate_against_registration(registration)?;
                    if lifecycle.diagnostic_table_key != registration.table_key {
                        return Err(OmniError::manifest(format!(
                            "stream lifecycle diagnostic alias '{}' does not match current binding '{}' for identity {}",
                            lifecycle.diagnostic_table_key,
                            registration.table_key,
                            lifecycle.identity
                        )));
                    }
                    if lifecycle_changes.insert(lifecycle.identity, ()).is_some() {
                        return Err(OmniError::manifest(format!(
                            "manifest batch changes stream lifecycle {} more than once",
                            lifecycle.identity
                        )));
                    }
                    if existing_stream_lifecycles
                        .get(&lifecycle.identity)
                        .is_some_and(|existing| existing == lifecycle)
                    {
                        continue;
                    }
                    rows.push(PendingVersionRow {
                        object_id: lifecycle.object_id(),
                        object_type: OBJECT_TYPE_STREAM_STATE.to_string(),
                        location: Some(lifecycle.binding.table_location.clone()),
                        metadata: Some(lifecycle.to_metadata_json()?),
                        table_key: lifecycle.diagnostic_table_key.clone(),
                        identity: Some(lifecycle.identity),
                        table_version: Some(lifecycle.current_head_witness.table_version),
                        table_branch: lifecycle.binding.table_branch.clone(),
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
        let mut table_identities = Vec::with_capacity(rows.len());
        let mut table_versions: Vec<Option<u64>> = Vec::with_capacity(rows.len());
        let mut table_branches = Vec::with_capacity(rows.len());
        let mut row_counts: Vec<Option<u64>> = Vec::with_capacity(rows.len());

        for row in rows {
            object_ids.push(row.object_id);
            object_types.push(row.object_type);
            locations.push(row.location);
            metadata.push(row.metadata);
            table_keys.push(row.table_key);
            table_identities.push(row.identity);
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
            table_identities,
            table_versions,
            table_branches,
            row_counts,
        )
    }

    /// Reduce the loaded `(identity, table_version) → entry` map and the
    /// tombstone set to "latest non-tombstoned version per identity" — the same
    /// reduction performed by `read_manifest_state` on the visible snapshot.
    /// Tombstoned tables fall back to their highest tombstone version so that
    /// the resulting `actual` reported in `ExpectedVersionMismatch` is
    /// meaningful even when the caller's expected table no longer exists.
    fn latest_visible_per_identity(
        existing_versions: &HashMap<(TableIdentity, u64), SubTableEntry>,
        existing_tombstones: &HashMap<(TableIdentity, u64), ()>,
    ) -> HashMap<TableIdentity, u64> {
        let mut max_tombstones = HashMap::<TableIdentity, u64>::new();
        for (identity, version) in existing_tombstones.keys() {
            max_tombstones
                .entry(*identity)
                .and_modify(|v| {
                    if *version > *v {
                        *v = *version;
                    }
                })
                .or_insert(*version);
        }

        let mut latest = HashMap::<TableIdentity, u64>::new();
        for (identity, version) in existing_versions.keys() {
            let tombstoned = max_tombstones
                .get(identity)
                .map(|t| *t >= *version)
                .unwrap_or(false);
            if tombstoned {
                continue;
            }
            latest
                .entry(*identity)
                .and_modify(|v| {
                    if *version > *v {
                        *v = *version;
                    }
                })
                .or_insert(*version);
        }

        // For tables that have only tombstones (no visible entry), surface the
        // tombstone version so callers see a non-zero `actual`.
        for (identity, tombstone) in &max_tombstones {
            latest.entry(*identity).or_insert(*tombstone);
        }

        latest
    }

    fn is_live_identity(
        identity: TableIdentity,
        existing_versions: &HashMap<(TableIdentity, u64), SubTableEntry>,
        existing_tombstones: &HashMap<(TableIdentity, u64), ()>,
    ) -> bool {
        let latest_version = existing_versions
            .keys()
            .filter_map(|(candidate, version)| (*candidate == identity).then_some(*version))
            .max();
        let latest_tombstone = existing_tombstones
            .keys()
            .filter_map(|(candidate, version)| (*candidate == identity).then_some(*version))
            .max();
        latest_version
            .map(|version| latest_tombstone.map(|t| t < version).unwrap_or(true))
            .unwrap_or(false)
    }

    /// Build the inputs for [`assemble_manifest_state`] from the pre-publish state
    /// unioned with the pending rows about to be committed — the in-memory basis
    /// for the post-publish `known_state` fold (RFC-013 PR2 #1b), so the caller
    /// skips the O(fragments) re-scan. Mirrors `read_manifest_scan`'s row handling
    /// exactly so the result is byte-identical: current aliases and paths resolve
    /// through `registrations` = `registered_tables` UNION the pending
    /// `OBJECT_TYPE_TABLE` rows (including a rename);
    /// `version_metadata` parses the SAME JSON string a re-scan would read. Pending
    /// `OBJECT_TYPE_TABLE` rows feed only `table_locations`; lineage rows
    /// (`graph_commit`/`graph_head`) are not manifest-state entries.
    fn fold_inputs(
        existing_versions: &HashMap<(TableIdentity, u64), SubTableEntry>,
        existing_tombstones: &HashMap<(TableIdentity, u64), ()>,
        rows: &[PendingVersionRow],
        registered_tables: &HashMap<TableIdentity, TableRegistration>,
        existing_stream_lifecycles: &HashMap<TableIdentity, StreamLifecycleEntry>,
    ) -> Result<(
        HashMap<TableIdentity, TableRegistration>,
        Vec<SubTableEntry>,
        Vec<(TableIdentity, u64)>,
        HashMap<TableIdentity, StreamLifecycleEntry>,
    )> {
        let mut registrations = registered_tables.clone();
        for row in rows {
            if row.object_type == OBJECT_TYPE_TABLE {
                let identity = row.identity.ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "post-publish fold: table row missing identity for {}",
                        row.table_key
                    ))
                })?;
                let location = row.location.clone().ok_or_else(|| {
                    OmniError::manifest_internal(format!(
                        "post-publish fold: table row missing path for {}",
                        row.table_key
                    ))
                })?;
                registrations.insert(
                    identity,
                    TableRegistration {
                        identity,
                        table_key: row.table_key.clone(),
                        table_path: location,
                    },
                );
            }
        }

        // Key version entries by `(identity, table_version)` so a pending row at
        // the SAME version REPLACES the pre-publish entry — modelling merge-insert
        // `UpdateAll` on the shared, deterministic `version_object_id(identity,
        // version)`. Load-bearing for the owner-branch handoff
        // (`is_owner_branch_handoff`): a handoff updates a `table_version` row in
        // place at the same version with a new `table_branch`, so `__manifest` ends
        // with ONE row carrying the new branch and a re-scan reflects it; appending
        // the pending row instead (and letting `assemble_manifest_state` keep the
        // first equal-version entry) would leave `known_state` on the stale fork.
        let mut version_map: HashMap<(TableIdentity, u64), SubTableEntry> =
            existing_versions.clone();
        let mut tombstones: Vec<(TableIdentity, u64)> = existing_tombstones
            .keys()
            .map(|(identity, version)| (*identity, *version))
            .collect();
        let mut stream_lifecycles = existing_stream_lifecycles.clone();

        for row in rows {
            match row.object_type.as_str() {
                OBJECT_TYPE_TABLE_VERSION => {
                    let identity = row.identity.ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "post-publish fold: table_version row missing identity for {}",
                            row.table_key
                        ))
                    })?;
                    let table_version = row.table_version.ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "post-publish fold: table_version row missing version for {}",
                            row.table_key
                        ))
                    })?;
                    let metadata_json = row.metadata.as_deref().ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "post-publish fold: table_version row missing metadata for {}",
                            row.table_key
                        ))
                    })?;
                    version_map.insert(
                        (identity, table_version),
                        SubTableEntry {
                            identity,
                            table_key: row.table_key.clone(),
                            table_path: String::new(),
                            table_version,
                            table_branch: row.table_branch.clone(),
                            row_count: row.row_count.ok_or_else(|| {
                                OmniError::manifest_internal(format!(
                                    "post-publish fold: table_version row missing row_count for {}",
                                    row.table_key
                                ))
                            })?,
                            version_metadata: TableVersionMetadata::from_json_str(metadata_json)?,
                        },
                    );
                }
                OBJECT_TYPE_TABLE_TOMBSTONE => {
                    let identity = row.identity.ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "post-publish fold: tombstone row missing identity for {}",
                            row.table_key
                        ))
                    })?;
                    let tombstone_version = row.table_version.ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "post-publish fold: tombstone row missing version for {}",
                            row.table_key
                        ))
                    })?;
                    tombstones.push((identity, tombstone_version));
                }
                OBJECT_TYPE_STREAM_STATE => {
                    let identity = row.identity.ok_or_else(|| {
                        OmniError::manifest_internal(
                            "post-publish fold: stream_state row missing identity",
                        )
                    })?;
                    let metadata = row.metadata.as_deref().ok_or_else(|| {
                        OmniError::manifest_internal(
                            "post-publish fold: stream_state row missing metadata",
                        )
                    })?;
                    let lifecycle = StreamLifecycleEntry::from_manifest_row(
                        &row.object_id,
                        &row.table_key,
                        identity,
                        row.location.as_deref(),
                        row.table_version,
                        row.table_branch.as_deref(),
                        metadata,
                    )?;
                    stream_lifecycles.insert(identity, lifecycle);
                }
                _ => {}
            }
        }

        Ok((
            registrations,
            version_map.into_values().collect(),
            tombstones,
            stream_lifecycles,
        ))
    }

    /// Compare each caller-supplied expectation against the manifest's current
    /// latest visible version per table. The first mismatch is returned as a
    /// typed `ExpectedVersionMismatch` (`actual = 0` if the table isn't in the
    /// manifest at all).
    fn check_expected_table_versions(
        latest_per_table: &HashMap<TableIdentity, u64>,
        registrations: &HashMap<TableIdentity, TableRegistration>,
        expected: &ExpectedTableVersions,
    ) -> Result<()> {
        for (identity, expectation) in expected {
            identity.validate()?;
            if let Some(registration) = registrations.get(identity) {
                if registration.table_key != expectation.table_key {
                    return Err(OmniError::manifest_read_set_changed(
                        format!("table_binding:{identity}"),
                        Some(expectation.table_key.clone()),
                        Some(registration.table_key.clone()),
                    ));
                }
            }
            let actual = latest_per_table.get(identity).copied().unwrap_or(0);
            if actual != expectation.table_version {
                return Err(OmniError::manifest_expected_version_mismatch(
                    expectation.table_key.clone(),
                    expectation.table_version,
                    actual,
                ));
            }
        }
        Ok(())
    }

    /// Check authority inside the publisher retry loop before pending rows are
    /// built. The graph head comes from the SAME scan used to build this CAS
    /// attempt; Lance's native branch identifier is re-read from the ref. Thus a
    /// row-level-CAS loser with an exact expectation cannot silently re-parent
    /// on its next attempt.
    async fn check_publish_precondition(
        &self,
        dataset: &Dataset,
        graph_heads: &HashMap<String, String>,
        precondition: &PublishPrecondition,
    ) -> Result<()> {
        let PublishPrecondition::ExactGraphHead(expected) = precondition else {
            return Ok(());
        };

        let expected_branch = expected
            .branch
            .as_deref()
            .filter(|branch| *branch != "main");
        if expected_branch != self.branch.as_deref() {
            return Err(OmniError::manifest_internal(format!(
                "publish graph-head precondition targets branch '{}' but publisher is bound to '{}'",
                expected_branch.unwrap_or("main"),
                self.branch.as_deref().unwrap_or("main"),
            )));
        }

        let branch_identity_member =
            format!("branch_identifier:{}", expected_branch.unwrap_or("main"));
        let expected_branch_identifier = serde_json::to_string(&expected.branch_identifier)
            .map_err(|e| {
                OmniError::manifest_internal(format!(
                    "failed to encode expected Lance branch identifier: {e}"
                ))
            })?;
        let actual_branch_identifier = match dataset.branch_identifier().await {
            Ok(identifier) => identifier,
            Err(LanceError::RefNotFound { .. }) => {
                return Err(OmniError::manifest_read_set_changed(
                    branch_identity_member,
                    Some(expected_branch_identifier),
                    None,
                ));
            }
            Err(err) => return Err(OmniError::Lance(err.to_string())),
        };
        if actual_branch_identifier != expected.branch_identifier {
            let actual = serde_json::to_string(&actual_branch_identifier).map_err(|e| {
                OmniError::manifest_internal(format!(
                    "failed to encode current Lance branch identifier: {e}"
                ))
            })?;
            return Err(OmniError::manifest_read_set_changed(
                branch_identity_member,
                Some(expected_branch_identifier),
                Some(actual),
            ));
        }

        let object_id = expected.object_id();
        let branch_key = object_id
            .strip_prefix("graph_head:")
            .expect("graph_head_object_id always supplies the prefix");
        let actual = graph_heads.get(branch_key).cloned();
        if actual != expected.head_commit_id {
            return Err(OmniError::manifest_read_set_changed(
                object_id,
                expected.head_commit_id.clone(),
                actual,
            ));
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
        let registrations = self.load_publish_state().await?.registered_tables;
        let changes = requests
            .iter()
            .cloned()
            .map(|request| {
                let (table_key, table_version, row_count, table_branch, version_metadata) =
                    parse_namespace_version_request(&request)
                        .map_err(|e| OmniError::Lance(e.to_string()))?;
                let identity = registrations
                    .values()
                    .find(|registration| registration.table_key == table_key)
                    .map(|registration| registration.identity)
                    .ok_or_else(|| {
                        OmniError::manifest(format!(
                            "test namespace request references unknown table alias {table_key}"
                        ))
                    })?;
                Ok(ManifestChange::Update(SubTableUpdate {
                    identity,
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
        identity: None,
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
    async fn publish_with_precondition(
        &self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<PublishOutcome> {
        if changes.is_empty()
            && expected_table_versions.is_empty()
            && lineage.is_none()
            && matches!(precondition, PublishPrecondition::Any)
        {
            // Defensive no-op (never reached from `commit_changes_with_lineage`,
            // which short-circuits the all-empty case): state is unchanged, so a
            // re-scan here is acceptable.
            let dataset = self.dataset().await?;
            let known_state = read_manifest_state(&dataset).await?;
            return Ok(PublishOutcome {
                dataset,
                parent_commit_id: None,
                known_state,
            });
        }

        for attempt in 0..=PUBLISHER_RETRY_BUDGET {
            // Route a retryable `load_publish_state` error through the SAME retry
            // path as a retryable `merge_rows` conflict below, so typed contention
            // composes with the publisher retry instead of aborting the publish.
            let loaded = match self.load_publish_state().await {
                Ok(loaded) => loaded,
                Err(err)
                    if attempt < PUBLISHER_RETRY_BUDGET && is_retryable_publish_conflict(&err) =>
                {
                    continue;
                }
                Err(err) => return Err(err),
            };
            let LoadedPublishState {
                dataset,
                registered_tables: known_tables,
                existing_versions,
                existing_tombstones,
                lineage_rows,
                graph_heads,
                stream_lifecycles,
            } = loaded;

            // Exact logical authority is checked on EVERY attempt from this
            // attempt's single manifest scan. In particular, a CAS retry after
            // another writer creates or advances `graph_head:<branch>` fails
            // here instead of transparently re-parenting the prepared intent.
            self.check_publish_precondition(&dataset, &graph_heads, precondition)
                .await?;

            let latest_per_table =
                Self::latest_visible_per_identity(&existing_versions, &existing_tombstones);
            // Pre-check on every attempt against freshly loaded state so a
            // concurrent commit that broke the caller's expectation is
            // surfaced as `ExpectedVersionMismatch` rather than retried.
            Self::check_expected_table_versions(
                &latest_per_table,
                &known_tables,
                expected_table_versions,
            )?;

            let mut rows = Self::build_pending_rows(
                changes,
                &known_tables,
                &existing_versions,
                &existing_tombstones,
                &stream_lifecycles,
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
                // the precondition held, nothing to write. Fold the unchanged state
                // from the loaded maps — no re-scan (RFC-013 PR2 #1b).
                let known_state = assemble_manifest_state(
                    dataset.version().version,
                    known_tables,
                    existing_versions.values().cloned().collect(),
                    existing_tombstones
                        .keys()
                        .map(|(identity, version)| (*identity, *version)),
                    graph_heads,
                    stream_lifecycles,
                )?;
                return Ok(PublishOutcome {
                    dataset,
                    parent_commit_id,
                    known_state,
                });
            }

            // Build the post-publish fold inputs from the pre-publish state ∪ the
            // rows we are about to commit, BEFORE `rows` is moved into merge_rows
            // (RFC-013 PR2 #1b). Recomputed per attempt from freshly-loaded state.
            let (fold_registrations, fold_entries, fold_tombstones, fold_stream_lifecycles) =
                Self::fold_inputs(
                    &existing_versions,
                    &existing_tombstones,
                    &rows,
                    &known_tables,
                    &stream_lifecycles,
                )?;
            let mut fold_graph_heads = graph_heads;
            if let Some(intent) = lineage {
                fold_graph_heads.insert(
                    intent
                        .branch
                        .as_deref()
                        .unwrap_or(MAIN_BRANCH_HEAD_KEY)
                        .to_string(),
                    intent.graph_commit_id.clone(),
                );
            }
            // Validate the complete post-batch fold before the physical merge.
            // In particular, alias collisions must fail without advancing
            // `__manifest`; discovering one after `merge_rows` would be an
            // acknowledged-but-unreadable manifest commit.
            let mut known_state = assemble_manifest_state(
                dataset.version().version + 1,
                fold_registrations,
                fold_entries,
                fold_tombstones,
                fold_graph_heads,
                fold_stream_lifecycles,
            )?;

            match self.merge_rows(dataset, rows).await {
                Ok(new_dataset) => {
                    known_state.version = new_dataset.version().version;
                    return Ok(PublishOutcome {
                        dataset: new_dataset,
                        parent_commit_id,
                        known_state,
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
