use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::BlobFile;
use lance::datatypes::BlobKind;
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::catalog::{Catalog, EdgeType, NodeType};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::types::ScalarType;

use crate::db::graph_coordinator::{GraphCoordinator, PublishedSnapshot};
use crate::db::run_registry::{RunRecord, RunStatus, is_internal_run_branch};
use crate::error::{OmniError, Result};
use crate::runtime_cache::RuntimeCache;
use crate::storage::{StorageAdapter, join_uri, normalize_root_uri, storage_for_uri};
use crate::table_store::TableStore;

use super::manifest::Snapshot;
use super::{ReadTarget, ResolvedTarget, RunId, SnapshotId};

const SCHEMA_FILENAME: &str = "_schema.pg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
}

/// Top-level handle to an Omnigraph database.
///
/// An Omnigraph is a Lance-native graph database with git-style branching.
/// It stores typed property graphs as per-type Lance datasets coordinated
/// through a Lance manifest table.
pub struct Omnigraph {
    root_uri: String,
    storage: Arc<dyn StorageAdapter>,
    coordinator: GraphCoordinator,
    table_store: TableStore,
    runtime_cache: RuntimeCache,
    catalog: Catalog,
    schema_source: String,
}

impl Omnigraph {
    /// Create a new repo at `uri` from schema source.
    ///
    /// Creates `_schema.pg`, per-type Lance datasets, and `_manifest.lance`.
    pub async fn init(uri: &str, schema_source: &str) -> Result<Self> {
        Self::init_with_storage(uri, schema_source, storage_for_uri(uri)?).await
    }

    pub(crate) async fn init_with_storage(
        uri: &str,
        schema_source: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri)?;
        // Parse and validate schema
        let schema_ast = parse_schema(schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        // Write _schema.pg
        let schema_path = join_uri(&root, SCHEMA_FILENAME);
        storage.write_text(&schema_path, schema_source).await?;

        // Create manifest + per-type datasets
        let coordinator = GraphCoordinator::init(&root, &catalog, Arc::clone(&storage)).await?;

        Ok(Self {
            root_uri: root.clone(),
            storage,
            coordinator,
            table_store: TableStore::new(&root),
            runtime_cache: RuntimeCache::default(),
            catalog,
            schema_source: schema_source.to_string(),
        })
    }

    /// Open an existing repo.
    ///
    /// Reads `_schema.pg`, parses it, builds the catalog, and opens the manifest.
    pub async fn open(uri: &str) -> Result<Self> {
        Self::open_with_storage(uri, storage_for_uri(uri)?).await
    }

    pub(crate) async fn open_with_storage(
        uri: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri)?;
        // Read _schema.pg
        let schema_path = join_uri(&root, SCHEMA_FILENAME);
        let schema_source = storage.read_text(&schema_path).await?;

        // Parse and validate schema
        let schema_ast = parse_schema(&schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        let coordinator = GraphCoordinator::open(&root, Arc::clone(&storage)).await?;

        Ok(Self {
            root_uri: root.clone(),
            storage,
            coordinator,
            table_store: TableStore::new(&root),
            runtime_cache: RuntimeCache::default(),
            catalog,
            schema_source,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn schema_source(&self) -> &str {
        &self.schema_source
    }

    pub fn uri(&self) -> &str {
        &self.root_uri
    }

    pub(crate) fn table_store(&self) -> &TableStore {
        &self.table_store
    }

    pub(crate) async fn open_coordinator_for_branch(
        &self,
        branch: Option<&str>,
    ) -> Result<GraphCoordinator> {
        match branch {
            Some(branch) => {
                GraphCoordinator::open_branch(self.uri(), branch, Arc::clone(&self.storage)).await
            }
            None => GraphCoordinator::open(self.uri(), Arc::clone(&self.storage)).await,
        }
    }

    pub(crate) async fn swap_coordinator_for_branch(
        &mut self,
        branch: Option<&str>,
    ) -> Result<GraphCoordinator> {
        let next = self.open_coordinator_for_branch(branch).await?;
        Ok(std::mem::replace(&mut self.coordinator, next))
    }

    pub(crate) fn restore_coordinator(&mut self, coordinator: GraphCoordinator) {
        self.coordinator = coordinator;
    }

    pub(crate) async fn resolved_branch_target(
        &self,
        branch: Option<&str>,
    ) -> Result<ResolvedTarget> {
        let requested = ReadTarget::Branch(branch.unwrap_or("main").to_string());
        let normalized = normalize_branch_name(branch.unwrap_or("main"))?;
        if normalized.as_deref() == self.coordinator.current_branch() {
            let snapshot_id = self.coordinator.head_commit_id().await?.unwrap_or_else(|| {
                SnapshotId::synthetic(
                    self.coordinator.current_branch(),
                    self.coordinator.version(),
                )
            });
            return Ok(ResolvedTarget {
                requested,
                branch: self.coordinator.current_branch().map(str::to_string),
                snapshot_id,
                snapshot: self.coordinator.snapshot(),
            });
        }
        self.coordinator.resolve_target(&requested).await
    }

    pub(crate) async fn snapshot_for_branch(&self, branch: Option<&str>) -> Result<Snapshot> {
        self.resolved_branch_target(branch)
            .await
            .map(|resolved| resolved.snapshot)
    }

    pub(crate) fn version(&self) -> u64 {
        self.coordinator.version()
    }

    /// Return an immutable Snapshot from the known manifest state. No storage I/O.
    pub(crate) fn snapshot(&self) -> Snapshot {
        self.coordinator.snapshot()
    }

    pub async fn snapshot_of(&self, target: impl Into<ReadTarget>) -> Result<Snapshot> {
        self.resolved_target(target)
            .await
            .map(|resolved| resolved.snapshot)
    }

    pub async fn version_of(&self, target: impl Into<ReadTarget>) -> Result<u64> {
        self.snapshot_of(target)
            .await
            .map(|snapshot| snapshot.version())
    }

    /// Synchronize this handle's write base to the latest head of the named branch.
    pub async fn sync_branch(&mut self, branch: &str) -> Result<()> {
        let branch = normalize_branch_name(branch)?;
        self.coordinator = self.open_coordinator_for_branch(branch.as_deref()).await?;
        self.runtime_cache.invalidate_all().await;
        Ok(())
    }

    /// Re-read the handle-local coordinator state from storage.
    pub(crate) async fn refresh(&mut self) -> Result<()> {
        self.coordinator.refresh().await?;
        self.runtime_cache.invalidate_all().await;
        Ok(())
    }

    pub async fn resolve_snapshot(&self, branch: &str) -> Result<SnapshotId> {
        self.coordinator.resolve_snapshot_id(branch).await
    }

    pub(crate) async fn resolved_target(
        &self,
        target: impl Into<ReadTarget>,
    ) -> Result<ResolvedTarget> {
        self.coordinator.resolve_target(&target.into()).await
    }

    // ─── Change detection ────────────────────────────────────────────────

    pub async fn diff_between(
        &self,
        from: impl Into<ReadTarget>,
        to: impl Into<ReadTarget>,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_resolved = self.resolved_target(from).await?;
        let to_resolved = self.resolved_target(to).await?;
        crate::changes::diff_snapshots(
            self.uri(),
            &from_resolved.snapshot,
            &to_resolved.snapshot,
            filter,
            to_resolved.branch.clone().or(from_resolved.branch.clone()),
        )
        .await
    }

    /// Diff two graph commits. Resolves each commit to `(manifest_branch, manifest_version)`
    /// and creates branch-aware snapshots. Supports cross-branch comparison.
    pub async fn diff_commits(
        &self,
        from_commit_id: &str,
        to_commit_id: &str,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_commit = self
            .coordinator
            .resolve_commit(&SnapshotId::new(from_commit_id))
            .await?;
        let to_commit = self
            .coordinator
            .resolve_commit(&SnapshotId::new(to_commit_id))
            .await?;
        let from_snap = self
            .coordinator
            .resolve_target(&ReadTarget::Snapshot(SnapshotId::new(
                from_commit.graph_commit_id.clone(),
            )))
            .await?;
        let to_snap = self
            .coordinator
            .resolve_target(&ReadTarget::Snapshot(SnapshotId::new(
                to_commit.graph_commit_id.clone(),
            )))
            .await?;
        crate::changes::diff_snapshots(
            self.uri(),
            &from_snap.snapshot,
            &to_snap.snapshot,
            filter,
            to_snap.branch.clone().or(from_snap.branch.clone()),
        )
        .await
    }

    pub async fn entity_at_target(
        &self,
        target: impl Into<ReadTarget>,
        table_key: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>> {
        let resolved = self.resolved_target(target).await?;
        self.entity_from_snapshot(&resolved.snapshot, table_key, id)
            .await
    }

    /// Read one entity at a specific manifest version via time travel (on-demand enrichment).
    pub async fn entity_at(
        &self,
        table_key: &str,
        id: &str,
        version: u64,
    ) -> Result<Option<serde_json::Value>> {
        let snap = self.coordinator.snapshot_at_version(version).await?;
        self.entity_from_snapshot(&snap, table_key, id).await
    }

    /// Create a Snapshot at any historical manifest version.
    pub async fn snapshot_at_version(&self, version: u64) -> Result<Snapshot> {
        self.coordinator.snapshot_at_version(version).await
    }

    async fn entity_from_snapshot(
        &self,
        snapshot: &Snapshot,
        table_key: &str,
        id: &str,
    ) -> Result<Option<serde_json::Value>> {
        if snapshot.entry(table_key).is_none() {
            return Ok(None);
        }

        let ds = self
            .table_store
            .open_snapshot_table(snapshot, table_key)
            .await?;
        let filter_sql = format!("id = '{}'", id.replace('\'', "''"));
        let batches = self
            .table_store
            .scan(&ds, None, Some(&filter_sql), None)
            .await?;
        let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
            return Ok(None);
        };
        Ok(Some(record_batch_row_to_json(batch, 0)?))
    }

    // ─── Graph index ──────────────────────────────────────────────────────

    /// Get or build the graph index for the current snapshot.
    pub async fn graph_index(&self) -> Result<Arc<crate::graph_index::GraphIndex>> {
        let resolved = self
            .coordinator
            .resolve_target(&ReadTarget::Branch(
                self.coordinator
                    .current_branch()
                    .unwrap_or("main")
                    .to_string(),
            ))
            .await?;
        self.runtime_cache
            .graph_index(&resolved, &self.catalog)
            .await
    }

    pub(crate) async fn graph_index_for_resolved(
        &self,
        resolved: &ResolvedTarget,
    ) -> Result<Arc<crate::graph_index::GraphIndex>> {
        self.runtime_cache
            .graph_index(resolved, &self.catalog)
            .await
    }

    /// Ensure BTree scalar indices exist on key columns.
    /// Idempotent — Lance skips if index already exists.
    ///
    /// Opens sub-tables at their latest version (not snapshot-pinned) because
    /// indices must be created on the current head. Any version drift from the
    /// snapshot is expected and logged. The resulting versions are committed
    /// back to the manifest.
    ///
    /// On named branches, indexing preserves lazy branching:
    /// unbranched subtables keep inheriting `main`, while subtables inherited
    /// from an ancestor branch are first forked into the active branch before
    /// their index metadata is updated.
    pub async fn ensure_indices(&mut self) -> Result<()> {
        let current_branch = self.coordinator.current_branch().map(str::to_string);
        self.ensure_indices_for_branch(current_branch.as_deref())
            .await
    }

    pub async fn ensure_indices_on(&mut self, branch: &str) -> Result<()> {
        let branch = normalize_branch_name(branch)?;
        self.ensure_indices_for_branch(branch.as_deref()).await
    }

    pub(crate) async fn ensure_indices_for_branch(&mut self, branch: Option<&str>) -> Result<()> {
        let resolved = self.resolved_branch_target(branch).await?;
        let snapshot = resolved.snapshot;
        let mut updates = Vec::new();
        let active_branch = resolved.branch;

        for type_name in self.catalog.node_types.keys() {
            let table_key = format!("node:{}", type_name);
            let Some(entry) = snapshot.entry(&table_key) else {
                continue;
            };
            let full_path = format!("{}/{}", self.root_uri, entry.table_path);
            let (mut ds, resolved_branch) = match active_branch.as_deref() {
                Some(active_branch) => match entry.table_branch.as_deref() {
                    None => continue,
                    _ => {
                        self.open_owned_dataset_for_branch_write(
                            &table_key,
                            &full_path,
                            entry.table_branch.as_deref(),
                            entry.table_version,
                            active_branch,
                        )
                        .await?
                    }
                },
                None => (self.open_dataset_head(&full_path, None).await?, None),
            };
            let row_count = self.table_store.count_rows(&ds, None).await.unwrap_or(0);
            if row_count > 0 {
                self.build_indices_on_dataset(&table_key, &mut ds).await?;
            }

            let final_version = self.table_store.dataset_version(&ds);
            if final_version != entry.table_version
                || resolved_branch.as_deref() != entry.table_branch.as_deref()
            {
                updates.push(crate::db::SubTableUpdate {
                    table_key,
                    table_version: final_version,
                    table_branch: resolved_branch,
                    row_count: row_count as u64,
                });
            }
        }

        for edge_name in self.catalog.edge_types.keys() {
            let table_key = format!("edge:{}", edge_name);
            let Some(entry) = snapshot.entry(&table_key) else {
                continue;
            };
            let full_path = format!("{}/{}", self.root_uri, entry.table_path);
            let (mut ds, resolved_branch) = match active_branch.as_deref() {
                Some(active_branch) => match entry.table_branch.as_deref() {
                    None => continue,
                    _ => {
                        self.open_owned_dataset_for_branch_write(
                            &table_key,
                            &full_path,
                            entry.table_branch.as_deref(),
                            entry.table_version,
                            active_branch,
                        )
                        .await?
                    }
                },
                None => (self.open_dataset_head(&full_path, None).await?, None),
            };
            let row_count = self.table_store.count_rows(&ds, None).await.unwrap_or(0);
            if row_count > 0 {
                self.build_indices_on_dataset(&table_key, &mut ds).await?;
            }

            let final_version = self.table_store.dataset_version(&ds);
            if final_version != entry.table_version
                || resolved_branch.as_deref() != entry.table_branch.as_deref()
            {
                updates.push(crate::db::SubTableUpdate {
                    table_key,
                    table_version: final_version,
                    table_branch: resolved_branch,
                    row_count: row_count as u64,
                });
            }
        }

        if !updates.is_empty() {
            self.commit_updates_on_branch(branch, &updates).await?;
        }

        Ok(())
    }

    /// Read a blob from a node by its string ID and property name.
    ///
    /// Returns a `BlobFile` handle with async `read()`, `seek()`, `tell()`,
    /// and metadata accessors (`size()`, `kind()`, `uri()`).
    ///
    /// ```ignore
    /// let blob = db.read_blob("Document", "readme", "content").await?;
    /// let bytes = blob.read().await?;
    /// ```
    pub async fn read_blob(&self, type_name: &str, id: &str, property: &str) -> Result<BlobFile> {
        let node_type = self
            .catalog
            .node_types
            .get(type_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;
        if !node_type.blob_properties.contains(property) {
            return Err(OmniError::manifest(format!(
                "property '{}' on type '{}' is not a Blob",
                property, type_name
            )));
        }

        let snapshot = self.snapshot();
        let table_key = format!("node:{}", type_name);
        let ds = snapshot.open(&table_key).await?;

        let filter_sql = format!("id = '{}'", id.replace('\'', "''"));
        let row_id = self
            .table_store
            .first_row_id_for_filter(&ds, &filter_sql)
            .await?
            .ok_or_else(|| {
                OmniError::manifest(format!("no {} with id '{}' found", type_name, id))
            })?;

        // Use take_blobs to get the BlobFile handle
        let ds = Arc::new(ds);
        let mut blobs = ds
            .take_blobs(&[row_id], property)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        blobs.pop().ok_or_else(|| {
            OmniError::manifest(format!(
                "blob '{}' on {} '{}' returned no data",
                property, type_name, id
            ))
        })
    }

    pub(crate) fn active_branch(&self) -> Option<&str> {
        self.coordinator.current_branch()
    }

    pub(crate) fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
        normalize_branch_name(branch)
    }

    pub(crate) async fn head_commit_id_for_branch(
        &self,
        branch: Option<&str>,
    ) -> Result<Option<String>> {
        let mut coordinator = self.open_coordinator_for_branch(branch).await?;
        coordinator.ensure_commit_graph_initialized().await?;
        coordinator
            .head_commit_id()
            .await
            .map(|id| id.map(|snapshot_id| snapshot_id.as_str().to_string()))
    }

    pub async fn branch_create(&mut self, name: &str) -> Result<()> {
        ensure_public_branch_ref(name, "branch_create")?;
        self.coordinator.branch_create(name).await
    }

    pub async fn branch_create_from(
        &mut self,
        from: impl Into<ReadTarget>,
        name: &str,
    ) -> Result<()> {
        self.branch_create_from_impl(from, name, false).await
    }

    async fn branch_create_from_impl(
        &mut self,
        from: impl Into<ReadTarget>,
        name: &str,
        allow_internal_refs: bool,
    ) -> Result<()> {
        let target = from.into();
        let ReadTarget::Branch(branch_name) = target else {
            return Err(OmniError::manifest(
                "branch creation from pinned snapshots is not supported yet".to_string(),
            ));
        };
        if !allow_internal_refs {
            ensure_public_branch_ref(&branch_name, "branch_create_from")?;
            ensure_public_branch_ref(name, "branch_create_from")?;
        }
        let branch = normalize_branch_name(&branch_name)?;
        let previous = self.swap_coordinator_for_branch(branch.as_deref()).await?;
        let result = self.coordinator.branch_create(name).await;
        self.restore_coordinator(previous);
        result
    }

    pub async fn branch_list(&self) -> Result<Vec<String>> {
        self.coordinator.branch_list().await
    }

    pub(crate) async fn latest_branch_snapshot_id(&self, branch: &str) -> Result<SnapshotId> {
        let normalized = normalize_branch_name(branch)?;
        let fresh = self
            .open_coordinator_for_branch(normalized.as_deref())
            .await?;
        fresh.resolve_snapshot_id(branch).await
    }

    pub async fn begin_run(
        &mut self,
        target_branch: &str,
        operation_hash: Option<&str>,
    ) -> Result<RunRecord> {
        ensure_public_branch_ref(target_branch, "begin_run")?;
        let target_branch =
            normalize_branch_name(target_branch)?.unwrap_or_else(|| "main".to_string());
        let fresh = self
            .open_coordinator_for_branch(Self::normalize_branch_name(&target_branch)?.as_deref())
            .await?;
        let base_snapshot_id = fresh.resolve_snapshot_id(&target_branch).await?;
        let base_manifest_version = fresh.version();
        let record = RunRecord::new(
            target_branch.clone(),
            base_snapshot_id.as_str(),
            base_manifest_version,
            operation_hash.map(str::to_string),
        )?;

        self.branch_create_from_impl(
            ReadTarget::branch(target_branch.clone()),
            &record.run_branch,
            true,
        )
        .await?;
        self.coordinator.append_run_record(&record).await?;
        Ok(record)
    }

    pub async fn get_run(&self, run_id: &RunId) -> Result<RunRecord> {
        self.coordinator.get_run(run_id).await
    }

    pub async fn list_runs(&self) -> Result<Vec<RunRecord>> {
        self.coordinator.list_runs().await
    }

    pub async fn abort_run(&mut self, run_id: &RunId) -> Result<RunRecord> {
        let run = self.get_run(run_id).await?;
        match run.status {
            RunStatus::Running | RunStatus::Failed => {
                let updated = run.with_status(RunStatus::Aborted, None)?;
                self.coordinator.append_run_record(&updated).await?;
                Ok(updated)
            }
            RunStatus::Published => Err(OmniError::manifest_conflict(format!(
                "run '{}' is already published",
                run_id
            ))),
            RunStatus::Aborted => Err(OmniError::manifest_conflict(format!(
                "run '{}' is already aborted",
                run_id
            ))),
        }
    }

    pub async fn fail_run(&mut self, run_id: &RunId) -> Result<RunRecord> {
        let run = self.get_run(run_id).await?;
        match run.status {
            RunStatus::Running => {
                let updated = run.with_status(RunStatus::Failed, None)?;
                self.coordinator.append_run_record(&updated).await?;
                Ok(updated)
            }
            RunStatus::Failed => Ok(run),
            RunStatus::Published => Err(OmniError::manifest_conflict(format!(
                "run '{}' is already published",
                run_id
            ))),
            RunStatus::Aborted => Err(OmniError::manifest_conflict(format!(
                "run '{}' is already aborted",
                run_id
            ))),
        }
    }

    pub async fn publish_run(&mut self, run_id: &RunId) -> Result<SnapshotId> {
        let run = self.get_run(run_id).await?;
        match run.status {
            RunStatus::Running => {}
            RunStatus::Published => {
                return run
                    .published_snapshot_id
                    .clone()
                    .map(SnapshotId::new)
                    .ok_or_else(|| {
                        OmniError::manifest(format!(
                            "run '{}' is published but missing published snapshot id",
                            run_id
                        ))
                    });
            }
            RunStatus::Failed | RunStatus::Aborted => {
                return Err(OmniError::manifest_conflict(format!(
                    "run '{}' is not publishable from status '{}'",
                    run_id,
                    run.status.as_str()
                )));
            }
        }

        let current_target_snapshot_id = self.resolve_snapshot(&run.target_branch).await?;
        if current_target_snapshot_id.as_str() == run.base_snapshot_id {
            self.sync_branch(&run.target_branch).await?;
            self.promote_run_snapshot_to_target(&run).await?;
        } else {
            self.branch_merge_internal(&run.run_branch, &run.target_branch)
                .await?;
            self.reify_internal_run_refs(&run.target_branch, &run.run_branch)
                .await?;
        }
        let published_snapshot_id = self.resolve_snapshot(&run.target_branch).await?;
        let updated = run.with_status(
            RunStatus::Published,
            Some(published_snapshot_id.as_str().to_string()),
        )?;
        self.coordinator.append_run_record(&updated).await?;
        Ok(published_snapshot_id)
    }

    async fn promote_run_snapshot_to_target(&mut self, run: &RunRecord) -> Result<()> {
        let target_snapshot = self
            .snapshot_of(ReadTarget::branch(run.target_branch.as_str()))
            .await?;
        let run_snapshot = self
            .snapshot_of(ReadTarget::branch(run.run_branch.as_str()))
            .await?;
        let mut table_keys = std::collections::BTreeSet::new();
        for entry in target_snapshot.entries() {
            table_keys.insert(entry.table_key.clone());
        }
        for entry in run_snapshot.entries() {
            table_keys.insert(entry.table_key.clone());
        }

        let mut updates = Vec::new();
        let mut changed_edge_tables = false;
        let target_branch = normalize_branch_name(&run.target_branch)?;

        for table_key in table_keys {
            let target_entry = target_snapshot.entry(&table_key);
            let run_entry = run_snapshot.entry(&table_key);
            if same_manifest_state(target_entry, run_entry) {
                continue;
            }
            let Some(_run_entry) = run_entry else {
                return Err(OmniError::manifest(format!(
                    "run '{}' removed table '{}' which publish_run does not support",
                    run.run_id, table_key
                )));
            };

            let source_ds = run_snapshot.open(&table_key).await?;
            let batch = self.batch_for_table_rewrite(&source_ds, &table_key).await?;

            let (mut target_ds, _full_path, table_branch) = self
                .open_for_mutation_on_branch(target_branch.as_deref(), &table_key)
                .await?;
            let state = self
                .table_store()
                .overwrite_batch(&mut target_ds, batch)
                .await?;
            updates.push(crate::db::SubTableUpdate {
                table_key: table_key.clone(),
                table_version: state.version,
                table_branch,
                row_count: state.row_count,
            });
            if table_key.starts_with("edge:") {
                changed_edge_tables = true;
            }
        }

        if !updates.is_empty() {
            self.commit_updates_on_branch(target_branch.as_deref(), &updates)
                .await?;
            if changed_edge_tables {
                self.invalidate_graph_index().await;
            }
        }

        Ok(())
    }

    async fn reify_internal_run_refs(
        &mut self,
        target_branch: &str,
        run_branch: &str,
    ) -> Result<()> {
        let target_snapshot = self.snapshot_of(ReadTarget::branch(target_branch)).await?;
        let mut updates = Vec::new();
        let mut changed_edge_tables = false;
        let target_branch = normalize_branch_name(target_branch)?;

        for entry in target_snapshot.entries() {
            if entry.table_branch.as_deref() != Some(run_branch) {
                continue;
            }

            let source_ds = target_snapshot.open(&entry.table_key).await?;
            let batch = self
                .batch_for_table_rewrite(&source_ds, &entry.table_key)
                .await?;

            let (mut target_ds, _full_path, table_branch) = self
                .open_for_mutation_on_branch(target_branch.as_deref(), &entry.table_key)
                .await?;
            let state = self
                .table_store()
                .overwrite_batch(&mut target_ds, batch)
                .await?;
            updates.push(crate::db::SubTableUpdate {
                table_key: entry.table_key.clone(),
                table_version: state.version,
                table_branch,
                row_count: state.row_count,
            });
            if entry.table_key.starts_with("edge:") {
                changed_edge_tables = true;
            }
        }

        if !updates.is_empty() {
            self.commit_updates_on_branch(target_branch.as_deref(), &updates)
                .await?;
            if changed_edge_tables {
                self.invalidate_graph_index().await;
            }
        }

        Ok(())
    }

    /// Open a sub-table for mutation with version-drift guard.
    ///
    /// Checks that the dataset's current version matches the snapshot-pinned
    /// version. If another writer has advanced the version, returns an error
    /// prompting the caller to refresh and retry (optimistic concurrency).
    pub(crate) async fn open_for_mutation(
        &self,
        table_key: &str,
    ) -> Result<(Dataset, String, Option<String>)> {
        let current_branch = self.coordinator.current_branch().map(str::to_string);
        self.open_for_mutation_on_branch(current_branch.as_deref(), table_key)
            .await
    }

    pub(crate) async fn open_for_mutation_on_branch(
        &self,
        branch: Option<&str>,
        table_key: &str,
    ) -> Result<(Dataset, String, Option<String>)> {
        let resolved = self.resolved_branch_target(branch).await?;
        let entry = resolved
            .snapshot
            .entry(table_key)
            .ok_or_else(|| OmniError::manifest(format!("no manifest entry for {}", table_key)))?;
        let full_path = format!("{}/{}", self.root_uri, entry.table_path);
        match resolved.branch.as_deref() {
            None => {
                let ds = self.open_dataset_head(&full_path, None).await?;
                self.table_store
                    .ensure_expected_version(&ds, table_key, entry.table_version)?;
                Ok((ds, full_path, None))
            }
            Some(active_branch) => {
                let (ds, table_branch) = self
                    .open_owned_dataset_for_branch_write(
                        table_key,
                        &full_path,
                        entry.table_branch.as_deref(),
                        entry.table_version,
                        active_branch,
                    )
                    .await?;
                Ok((ds, full_path, table_branch))
            }
        }
    }

    /// Open the dataset that should receive a branch-local metadata or data
    /// write, forking it from the manifest-pinned source state when the active
    /// branch does not yet own the subtable.
    pub(crate) async fn open_owned_dataset_for_branch_write(
        &self,
        table_key: &str,
        full_path: &str,
        entry_branch: Option<&str>,
        entry_version: u64,
        active_branch: &str,
    ) -> Result<(Dataset, Option<String>)> {
        match entry_branch {
            Some(branch) if branch == active_branch => {
                let ds = self
                    .open_dataset_head(full_path, Some(active_branch))
                    .await?;
                self.table_store
                    .ensure_expected_version(&ds, table_key, entry_version)?;
                Ok((ds, Some(active_branch.to_string())))
            }
            source_branch => {
                let ds = self
                    .fork_dataset_from_entry_state(
                        table_key,
                        full_path,
                        source_branch,
                        entry_version,
                        active_branch,
                    )
                    .await?;
                Ok((ds, Some(active_branch.to_string())))
            }
        }
    }

    pub(crate) async fn fork_dataset_from_entry_state(
        &self,
        table_key: &str,
        full_path: &str,
        source_branch: Option<&str>,
        source_version: u64,
        active_branch: &str,
    ) -> Result<Dataset> {
        let ds = self
            .table_store
            .fork_branch_from_state(
                full_path,
                source_branch,
                table_key,
                source_version,
                active_branch,
            )
            .await?;
        Ok(ds)
    }

    pub(crate) async fn reopen_for_mutation(
        &self,
        table_key: &str,
        full_path: &str,
        table_branch: Option<&str>,
        expected_version: u64,
    ) -> Result<Dataset> {
        self.table_store
            .reopen_for_mutation(full_path, table_branch, table_key, expected_version)
            .await
    }

    pub(crate) async fn open_dataset_head(
        &self,
        full_path: &str,
        branch: Option<&str>,
    ) -> Result<Dataset> {
        self.table_store.open_dataset_head(full_path, branch).await
    }

    pub(crate) async fn open_dataset_at_state(
        &self,
        table_path: &str,
        table_branch: Option<&str>,
        table_version: u64,
    ) -> Result<Dataset> {
        self.table_store
            .open_dataset_at_state(table_path, table_branch, table_version)
            .await
    }

    pub(crate) async fn build_indices_on_dataset(
        &self,
        table_key: &str,
        ds: &mut Dataset,
    ) -> Result<()> {
        if let Some(type_name) = table_key.strip_prefix("node:") {
            self.table_store
                .create_btree_index(ds, &["id"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(id): {}", table_key, e))
                })?;

            if let Some(node_type) = self.catalog.node_types.get(type_name) {
                for index_cols in &node_type.indices {
                    if index_cols.len() != 1 {
                        continue;
                    }
                    let prop_name = &index_cols[0];
                    if let Some(prop_type) = node_type.properties.get(prop_name) {
                        if matches!(prop_type.scalar, ScalarType::String) && !prop_type.list {
                            self.table_store
                                .create_inverted_index(ds, prop_name.as_str())
                                .await
                                .map_err(|e| {
                                    OmniError::Lance(format!(
                                        "create Inverted index on {}({}): {}",
                                        table_key, prop_name, e
                                    ))
                                })?;
                        } else if matches!(prop_type.scalar, ScalarType::Vector(_))
                            && !prop_type.list
                        {
                            self.table_store
                                .create_vector_index(ds, prop_name.as_str())
                                .await
                                .map_err(|e| {
                                    OmniError::Lance(format!(
                                        "create Vector index on {}({}): {}",
                                        table_key, prop_name, e
                                    ))
                                })?;
                        }
                    }
                }
            }
            return Ok(());
        }

        if table_key.starts_with("edge:") {
            self.table_store
                .create_btree_index(ds, &["id"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(id): {}", table_key, e))
                })?;
            self.table_store
                .create_btree_index(ds, &["src"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(src): {}", table_key, e))
                })?;
            self.table_store
                .create_btree_index(ds, &["dst"])
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(dst): {}", table_key, e))
                })?;
            return Ok(());
        }

        Err(OmniError::manifest(format!(
            "invalid table key '{}'",
            table_key
        )))
    }

    pub(crate) async fn commit_updates(
        &mut self,
        updates: &[crate::db::SubTableUpdate],
    ) -> Result<u64> {
        let PublishedSnapshot {
            manifest_version,
            _snapshot_id: _,
        } = self.coordinator.commit_updates(updates).await?;
        Ok(manifest_version)
    }

    pub(crate) async fn commit_manifest_updates(
        &mut self,
        updates: &[crate::db::SubTableUpdate],
    ) -> Result<u64> {
        self.coordinator.commit_manifest_updates(updates).await
    }

    pub(crate) async fn record_merge_commit(
        &mut self,
        manifest_version: u64,
        parent_commit_id: &str,
        merged_parent_commit_id: &str,
    ) -> Result<String> {
        self.coordinator
            .record_merge_commit(manifest_version, parent_commit_id, merged_parent_commit_id)
            .await
            .map(|snapshot_id| snapshot_id.as_str().to_string())
    }

    pub(crate) async fn commit_updates_on_branch(
        &mut self,
        branch: Option<&str>,
        updates: &[crate::db::SubTableUpdate],
    ) -> Result<u64> {
        let current_branch = self.coordinator.current_branch().map(str::to_string);
        let requested_branch = branch.map(|branch| branch.to_string());
        if requested_branch == current_branch {
            return self.commit_updates(updates).await;
        }

        let mut coordinator = match requested_branch.as_deref() {
            Some(branch) => {
                GraphCoordinator::open_branch(self.uri(), branch, Arc::clone(&self.storage)).await?
            }
            None => GraphCoordinator::open(self.uri(), Arc::clone(&self.storage)).await?,
        };
        let PublishedSnapshot {
            manifest_version,
            _snapshot_id: _,
        } = coordinator.commit_updates(updates).await?;
        Ok(manifest_version)
    }

    pub(crate) async fn ensure_commit_graph_initialized(&mut self) -> Result<()> {
        self.coordinator.ensure_commit_graph_initialized().await
    }

    /// Invalidate the cached graph index. Called after edge mutations.
    pub(crate) async fn invalidate_graph_index(&self) {
        self.runtime_cache.invalidate_all().await;
    }

    async fn batch_for_table_rewrite(
        &self,
        source_ds: &Dataset,
        table_key: &str,
    ) -> Result<RecordBatch> {
        let target_schema = schema_for_table_key(self.catalog(), table_key)?;
        let blob_properties = blob_properties_for_table_key(self.catalog(), table_key)?;
        if blob_properties.is_empty() {
            let batches = self.table_store().scan_batches(source_ds).await?;
            return concat_or_empty_batches(target_schema, batches);
        }

        let batches = self
            .table_store()
            .scan_with(source_ds, None, None, None, true, |_| Ok(()))
            .await?;
        let batch = concat_or_empty_batches(target_schema.clone(), batches)?;
        if batch.num_rows() == 0 {
            return Ok(batch);
        }

        let row_ids = batch
            .column_by_name("_rowid")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| {
                OmniError::Lance(format!(
                    "expected _rowid column when rewriting '{}'",
                    table_key
                ))
            })?;
        let row_ids: Vec<u64> = row_ids.values().iter().copied().collect();

        let mut columns = Vec::with_capacity(target_schema.fields().len());
        for field in target_schema.fields() {
            if blob_properties.contains(field.name()) {
                let descriptions = batch
                    .column_by_name(field.name())
                    .and_then(|col| col.as_any().downcast_ref::<StructArray>())
                    .ok_or_else(|| {
                        OmniError::Lance(format!(
                            "expected blob descriptions for '{}.{}'",
                            table_key,
                            field.name()
                        ))
                    })?;
                columns.push(
                    self.rebuild_blob_column(source_ds, field.name(), descriptions, &row_ids)
                        .await?,
                );
            } else {
                columns.push(batch.column_by_name(field.name()).cloned().ok_or_else(|| {
                    OmniError::Lance(format!(
                        "missing column '{}.{}' in rewrite batch",
                        table_key,
                        field.name()
                    ))
                })?);
            }
        }

        RecordBatch::try_new(target_schema, columns).map_err(|e| OmniError::Lance(e.to_string()))
    }

    async fn rebuild_blob_column(
        &self,
        source_ds: &Dataset,
        column_name: &str,
        descriptions: &StructArray,
        row_ids: &[u64],
    ) -> Result<Arc<dyn Array>> {
        let mut builder = BlobArrayBuilder::new(row_ids.len());
        let mut non_null_row_ids = Vec::new();
        let mut row_has_blob = Vec::with_capacity(row_ids.len());

        for row in 0..row_ids.len() {
            let is_null = blob_description_is_null(descriptions, row)?;
            row_has_blob.push(!is_null);
            if !is_null {
                non_null_row_ids.push(row_ids[row]);
            }
        }

        let blob_files = if non_null_row_ids.is_empty() {
            Vec::new()
        } else {
            Arc::new(source_ds.clone())
                .take_blobs(&non_null_row_ids, column_name)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))?
        };

        let mut files = blob_files.into_iter();
        for has_blob in row_has_blob {
            if !has_blob {
                builder
                    .push_null()
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
                continue;
            }

            let blob = files.next().ok_or_else(|| {
                OmniError::Lance(format!(
                    "blob rewrite for '{}' lost alignment with source rows",
                    column_name
                ))
            })?;
            if let Some(uri) = blob.uri() {
                builder
                    .push_uri(uri)
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            } else {
                builder
                    .push_bytes(
                        blob.read()
                            .await
                            .map_err(|e| OmniError::Lance(e.to_string()))?,
                    )
                    .map_err(|e| OmniError::Lance(e.to_string()))?;
            }
        }

        if files.next().is_some() {
            return Err(OmniError::Lance(format!(
                "blob rewrite for '{}' produced extra source blobs",
                column_name
            )));
        }

        builder
            .finish()
            .map_err(|e| OmniError::Lance(e.to_string()))
    }
}

pub(crate) fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
    let branch = branch.trim();
    if branch.is_empty() {
        return Err(OmniError::manifest(
            "branch name cannot be empty".to_string(),
        ));
    }
    if branch == "main" {
        return Ok(None);
    }
    Ok(Some(branch.to_string()))
}

fn ensure_public_branch_ref(branch: &str, operation: &str) -> Result<()> {
    if is_internal_run_branch(branch) {
        return Err(OmniError::manifest(format!(
            "{} does not allow internal run ref '{}'",
            operation, branch
        )));
    }
    Ok(())
}

fn same_manifest_state(
    left: Option<&crate::db::SubTableEntry>,
    right: Option<&crate::db::SubTableEntry>,
) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => {
            left.table_path == right.table_path
                && left.table_version == right.table_version
                && left.table_branch == right.table_branch
                && left.row_count == right.row_count
        }
        _ => false,
    }
}

fn concat_or_empty_batches(schema: Arc<Schema>, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
    }
    let batch_schema = batches[0].schema();
    arrow_select::concat::concat_batches(&batch_schema, &batches)
        .map_err(|e| OmniError::Lance(e.to_string()))
}

fn blob_properties_for_table_key<'a>(
    catalog: &'a Catalog,
    table_key: &str,
) -> Result<&'a std::collections::HashSet<String>> {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        return catalog
            .node_types
            .get(type_name)
            .map(|node_type| &node_type.blob_properties)
            .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)));
    }
    if let Some(type_name) = table_key.strip_prefix("edge:") {
        return catalog
            .edge_types
            .get(type_name)
            .map(|edge_type| &edge_type.blob_properties)
            .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", type_name)));
    }
    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

fn blob_description_is_null(descriptions: &StructArray, row: usize) -> Result<bool> {
    if descriptions.is_null(row) {
        return Ok(true);
    }

    let kind = descriptions
        .column_by_name("kind")
        .and_then(|col| col.as_any().downcast_ref::<UInt32Array>())
        .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row) as u8))
        .or_else(|| {
            descriptions
                .column_by_name("kind")
                .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt8Array>())
                .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row)))
        });
    let position = descriptions
        .column_by_name("position")
        .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
        .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row)));
    let size = descriptions
        .column_by_name("size")
        .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
        .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row)));
    let blob_uri = descriptions
        .column_by_name("blob_uri")
        .and_then(|col| col.as_any().downcast_ref::<StringArray>())
        .and_then(|arr| (!arr.is_null(row)).then(|| arr.value(row)));

    let Some(kind) = kind else {
        return Ok(true);
    };
    let kind = BlobKind::try_from(kind).map_err(|e| OmniError::Lance(e.to_string()))?;
    if kind != BlobKind::Inline {
        return Ok(false);
    }

    Ok(position.unwrap_or(0) == 0 && size.unwrap_or(0) == 0 && blob_uri.unwrap_or("").is_empty())
}

/// Replace placeholder `LargeBinary` fields with Lance blob v2 fields.
///
/// The compiler crate has no Lance dependency, so `ScalarType::Blob` maps to
/// `DataType::LargeBinary` as a placeholder. This function replaces those
/// fields with the real blob v2 struct type via `lance::blob::blob_field()`.
fn fixup_blob_schemas(catalog: &mut Catalog) {
    for node_type in catalog.node_types.values_mut() {
        if node_type.blob_properties.is_empty() {
            continue;
        }
        let fields: Vec<Field> = node_type
            .arrow_schema
            .fields()
            .iter()
            .map(|f| {
                if node_type.blob_properties.contains(f.name()) {
                    blob_field(f.name(), f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        node_type.arrow_schema = Arc::new(Schema::new(fields));
    }
    for edge_type in catalog.edge_types.values_mut() {
        if edge_type.blob_properties.is_empty() {
            continue;
        }
        let fields: Vec<Field> = edge_type
            .arrow_schema
            .fields()
            .iter()
            .map(|f| {
                if edge_type.blob_properties.contains(f.name()) {
                    blob_field(f.name(), f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        edge_type.arrow_schema = Arc::new(Schema::new(fields));
    }
}

fn schema_for_table_key(catalog: &Catalog, table_key: &str) -> Result<Arc<Schema>> {
    if let Some(type_name) = table_key.strip_prefix("node:") {
        let node_type: &NodeType = catalog
            .node_types
            .get(type_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown node type '{}'", type_name)))?;
        return Ok(node_type.arrow_schema.clone());
    }
    if let Some(type_name) = table_key.strip_prefix("edge:") {
        let edge_type: &EdgeType = catalog
            .edge_types
            .get(type_name)
            .ok_or_else(|| OmniError::manifest(format!("unknown edge type '{}'", type_name)))?;
        return Ok(edge_type.arrow_schema.clone());
    }
    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

fn record_batch_row_to_json(batch: &RecordBatch, row: usize) -> Result<serde_json::Value> {
    let mut obj = serde_json::Map::new();
    for (i, field) in batch.schema().fields().iter().enumerate() {
        obj.insert(
            field.name().clone(),
            json_value_from_array(batch.column(i).as_ref(), row)?,
        );
    }
    Ok(serde_json::Value::Object(obj))
}

fn json_value_from_array(array: &dyn Array, row: usize) -> Result<serde_json::Value> {
    if array.is_null(row) {
        return Ok(serde_json::Value::Null);
    }

    match array.data_type() {
        DataType::Utf8 => Ok(serde_json::Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| OmniError::Lance("expected StringArray".to_string()))?
                .value(row)
                .to_string(),
        )),
        DataType::LargeUtf8 => Ok(serde_json::Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| OmniError::Lance("expected LargeStringArray".to_string()))?
                .value(row)
                .to_string(),
        )),
        DataType::Boolean => Ok(serde_json::Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| OmniError::Lance("expected BooleanArray".to_string()))?
                .value(row),
        )),
        DataType::Int32 => Ok(serde_json::Value::Number(serde_json::Number::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| OmniError::Lance("expected Int32Array".to_string()))?
                .value(row),
        ))),
        DataType::Int64 => Ok(serde_json::Value::Number(serde_json::Number::from(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| OmniError::Lance("expected Int64Array".to_string()))?
                .value(row),
        ))),
        DataType::UInt32 => Ok(serde_json::Value::Number(serde_json::Number::from(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| OmniError::Lance("expected UInt32Array".to_string()))?
                .value(row),
        ))),
        DataType::UInt64 => Ok(serde_json::Value::Number(serde_json::Number::from(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| OmniError::Lance("expected UInt64Array".to_string()))?
                .value(row),
        ))),
        DataType::Float32 => {
            let value = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| OmniError::Lance("expected Float32Array".to_string()))?
                .value(row) as f64;
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(value).ok_or_else(|| {
                    OmniError::Lance(format!("cannot encode f32 value '{}' as JSON", value))
                })?,
            ))
        }
        DataType::Float64 => {
            let value = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| OmniError::Lance("expected Float64Array".to_string()))?
                .value(row);
            Ok(serde_json::Value::Number(
                serde_json::Number::from_f64(value).ok_or_else(|| {
                    OmniError::Lance(format!("cannot encode f64 value '{}' as JSON", value))
                })?,
            ))
        }
        DataType::Date32 => Ok(serde_json::Value::Number(serde_json::Number::from(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| OmniError::Lance("expected Date32Array".to_string()))?
                .value(row),
        ))),
        DataType::Binary => Ok(serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| OmniError::Lance("expected BinaryArray".to_string()))?
                .value(row),
        ))),
        DataType::LargeBinary => Ok(serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| OmniError::Lance("expected LargeBinaryArray".to_string()))?
                .value(row),
        ))),
        DataType::List(_) => {
            let list = array
                .as_any()
                .downcast_ref::<ListArray>()
                .ok_or_else(|| OmniError::Lance("expected ListArray".to_string()))?;
            let values = list.value(row);
            let mut out = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                out.push(json_value_from_array(values.as_ref(), idx)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        DataType::LargeList(_) => {
            let list = array
                .as_any()
                .downcast_ref::<LargeListArray>()
                .ok_or_else(|| OmniError::Lance("expected LargeListArray".to_string()))?;
            let values = list.value(row);
            let mut out = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                out.push(json_value_from_array(values.as_ref(), idx)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        DataType::FixedSizeList(_, _) => {
            let list = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| OmniError::Lance("expected FixedSizeListArray".to_string()))?;
            let values = list.value(row);
            let mut out = Vec::with_capacity(values.len());
            for idx in 0..values.len() {
                out.push(json_value_from_array(values.as_ref(), idx)?);
            }
            Ok(serde_json::Value::Array(out))
        }
        DataType::Struct(fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| OmniError::Lance("expected StructArray".to_string()))?;
            let mut obj = serde_json::Map::new();
            for (field_idx, field) in fields.iter().enumerate() {
                obj.insert(
                    field.name().clone(),
                    json_value_from_array(struct_array.column(field_idx).as_ref(), row)?,
                );
            }
            Ok(serde_json::Value::Object(obj))
        }
        _ => {
            let value = arrow_cast::display::array_value_to_string(array, row)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            Ok(serde_json::Value::String(value))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::Mutex;

    use crate::storage::{LocalStorageAdapter, StorageAdapter, join_uri};

    const TEST_SCHEMA: &str = r#"
node Person {
    name: String @key
    age: I32?
}
node Company {
    name: String @key
}
edge Knows: Person -> Person {
    since: Date?
}
edge WorksAt: Person -> Company
"#;

    #[derive(Debug, Default)]
    struct RecordingStorageAdapter {
        inner: LocalStorageAdapter,
        reads: Mutex<Vec<String>>,
        writes: Mutex<Vec<String>>,
        exists_checks: Mutex<Vec<String>>,
    }

    impl RecordingStorageAdapter {
        fn reads(&self) -> Vec<String> {
            self.reads.lock().unwrap().clone()
        }

        fn writes(&self) -> Vec<String> {
            self.writes.lock().unwrap().clone()
        }

        fn exists_checks(&self) -> Vec<String> {
            self.exists_checks.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl StorageAdapter for RecordingStorageAdapter {
        async fn read_text(&self, uri: &str) -> Result<String> {
            self.reads.lock().unwrap().push(uri.to_string());
            self.inner.read_text(uri).await
        }

        async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
            self.writes.lock().unwrap().push(uri.to_string());
            self.inner.write_text(uri, contents).await
        }

        async fn exists(&self, uri: &str) -> Result<bool> {
            self.exists_checks.lock().unwrap().push(uri.to_string());
            self.inner.exists(uri).await
        }
    }

    #[tokio::test]
    async fn test_init_creates_repo() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        // Schema file written
        assert!(dir.path().join("_schema.pg").exists());

        // Manifest created with correct entries
        let snap = db.snapshot();
        assert!(snap.entry("node:Person").is_some());
        assert!(snap.entry("node:Company").is_some());
        assert!(snap.entry("edge:Knows").is_some());
        assert!(snap.entry("edge:WorksAt").is_some());

        // Catalog is correct
        assert_eq!(db.catalog().node_types.len(), 2);
        assert_eq!(db.catalog().edge_types.len(), 2);
        assert_eq!(
            db.catalog().node_types["Person"].key_property(),
            Some("name")
        );
    }

    #[tokio::test]
    async fn test_open_reads_existing_repo() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        // Re-open
        let db = Omnigraph::open(uri).await.unwrap();
        assert_eq!(db.catalog().node_types.len(), 2);
        assert_eq!(db.catalog().edge_types.len(), 2);
        let snap = db.snapshot();
        assert!(snap.entry("node:Person").is_some());
        assert!(snap.entry("edge:Knows").is_some());
    }

    #[tokio::test]
    async fn test_init_and_open_route_graph_metadata_through_storage_adapter() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let adapter = Arc::new(RecordingStorageAdapter::default());

        Omnigraph::init_with_storage(uri, TEST_SCHEMA, adapter.clone())
            .await
            .unwrap();
        assert!(adapter.writes().contains(&join_uri(uri, "_schema.pg")));

        Omnigraph::open_with_storage(uri, adapter.clone())
            .await
            .unwrap();
        assert!(adapter.reads().contains(&join_uri(uri, "_schema.pg")));
        assert!(
            adapter
                .exists_checks()
                .contains(&join_uri(uri, "_graph_commits.lance"))
        );
    }

    #[tokio::test]
    async fn test_open_nonexistent_fails() {
        let result = Omnigraph::open("/tmp/nonexistent_omnigraph_test_xyz").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_snapshot_version_is_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();

        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        // Take snapshot before any writes
        let snap1 = db.snapshot();
        let v1 = snap1.version();

        // Load data — advances manifest version
        crate::loader::load_jsonl(
            &mut db,
            r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#,
            crate::loader::LoadMode::Overwrite,
        )
        .await
        .unwrap();

        // Snapshot from handle sees new version
        let snap2 = db.snapshot();
        assert!(snap2.version() > v1);

        // But the old snapshot is still pinned
        assert_eq!(snap1.version(), v1);
    }
}
