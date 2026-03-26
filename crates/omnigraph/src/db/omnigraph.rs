use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::blob::blob_field;
use lance::dataset::BlobFile;
use lance::index::vector::VectorIndexParams;
use lance_index::scalar::{InvertedIndexParams, ScalarIndexParams};
use lance_index::{DatasetIndexExt, IndexType};
use lance_linalg::distance::MetricType;
use omnigraph_compiler::build_catalog;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::types::ScalarType;

use crate::db::graph_coordinator::{GraphCoordinator, PublishedSnapshot};
use crate::error::{OmniError, Result};
use crate::runtime_cache::RuntimeCache;
use crate::storage::{StorageAdapter, default_storage, join_uri, normalize_root_uri};
use crate::table_store::TableStore;

use super::manifest::Snapshot;
use super::{ReadTarget, ResolvedTarget, SnapshotId};

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
        Self::init_with_storage(uri, schema_source, default_storage()).await
    }

    pub(crate) async fn init_with_storage(
        uri: &str,
        schema_source: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri);
        // Parse and validate schema
        let schema_ast = parse_schema(schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        // Write _schema.pg
        let schema_path = join_uri(&root, SCHEMA_FILENAME);
        storage.write_text(&schema_path, schema_source)?;

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
        Self::open_with_storage(uri, default_storage()).await
    }

    pub(crate) async fn open_with_storage(
        uri: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri);
        // Read _schema.pg
        let schema_path = join_uri(&root, SCHEMA_FILENAME);
        let schema_source = storage.read_text(&schema_path)?;

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

    pub async fn open_branch(uri: &str, branch: &str) -> Result<Self> {
        Self::open_branch_with_storage(uri, branch, default_storage()).await
    }

    pub(crate) async fn open_branch_with_storage(
        uri: &str,
        branch: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri);
        let schema_path = join_uri(&root, SCHEMA_FILENAME);
        let schema_source = storage.read_text(&schema_path)?;

        let schema_ast = parse_schema(&schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        let coordinator =
            GraphCoordinator::open_branch(&root, branch, Arc::clone(&storage)).await?;

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

    pub(crate) async fn reopen_for_branch(&self, branch: Option<&str>) -> Result<Self> {
        match branch {
            Some(branch) => {
                Omnigraph::open_branch_with_storage(self.uri(), branch, Arc::clone(&self.storage))
                    .await
            }
            None => Omnigraph::open_with_storage(self.uri(), Arc::clone(&self.storage)).await,
        }
    }

    pub fn version(&self) -> u64 {
        self.coordinator.version()
    }

    /// Return an immutable Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        self.coordinator.snapshot()
    }

    /// Re-read manifest from storage to see other writers' commits.
    pub async fn refresh(&mut self) -> Result<()> {
        self.coordinator.refresh().await?;
        self.runtime_cache.invalidate_all();
        Ok(())
    }

    pub async fn resolve_snapshot(&self, branch: &str) -> Result<SnapshotId> {
        self.coordinator.resolve_snapshot_id(branch).await
    }

    pub async fn resolved_target(&self, target: impl Into<ReadTarget>) -> Result<ResolvedTarget> {
        self.coordinator.resolve_target(&target.into()).await
    }

    // ─── Change detection ────────────────────────────────────────────────

    /// Diff two manifest versions on the current branch.
    pub async fn diff(
        &self,
        from_version: u64,
        to_version: u64,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_snap = self.coordinator.snapshot_at_version(from_version).await?;
        let to_snap = self.coordinator.snapshot_at_version(to_version).await?;
        crate::changes::diff_snapshots(
            self.uri(),
            &from_snap,
            &to_snap,
            filter,
            self.coordinator.current_branch().map(str::to_string),
        )
        .await
    }

    /// Changes since a version on the current branch. Sugar for `diff(from, current)`.
    pub async fn changes_since(
        &self,
        from_version: u64,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_snap = self.coordinator.snapshot_at_version(from_version).await?;
        let to_snap = self.snapshot();
        crate::changes::diff_snapshots(
            self.uri(),
            &from_snap,
            &to_snap,
            filter,
            self.coordinator.current_branch().map(str::to_string),
        )
        .await
    }

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
        let mut scanner = ds.scan();
        scanner
            .filter(&filter_sql)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let batches: Vec<RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let Some(batch) = batches.iter().find(|batch| batch.num_rows() > 0) else {
            return Ok(None);
        };
        Ok(Some(record_batch_row_to_json(batch, 0)?))
    }

    // ─── Graph index ──────────────────────────────────────────────────────

    /// Get or build the graph index for the current snapshot.
    pub async fn graph_index(&mut self) -> Result<Arc<crate::graph_index::GraphIndex>> {
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

    pub(crate) async fn graph_index_for_target(
        &mut self,
        target: impl Into<ReadTarget>,
    ) -> Result<Arc<crate::graph_index::GraphIndex>> {
        let resolved = self.resolved_target(target).await?;
        self.runtime_cache
            .graph_index(&resolved, &self.catalog)
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
        let snapshot = self.snapshot();
        let mut updates = Vec::new();
        let active_branch = self.coordinator.current_branch().map(str::to_string);

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
            let row_count = ds.count_rows(None).await.unwrap_or(0);
            if row_count > 0 {
                self.build_indices_on_dataset(&table_key, &mut ds).await?;
            }

            let final_version = ds.version().version;
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
            let row_count = ds.count_rows(None).await.unwrap_or(0);
            if row_count > 0 {
                self.build_indices_on_dataset(&table_key, &mut ds).await?;
            }

            let final_version = ds.version().version;
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
            self.commit_updates(&updates).await?;
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
            .ok_or_else(|| OmniError::Manifest(format!("unknown node type '{}'", type_name)))?;
        if !node_type.blob_properties.contains(property) {
            return Err(OmniError::Manifest(format!(
                "property '{}' on type '{}' is not a Blob",
                property, type_name
            )));
        }

        let snapshot = self.snapshot();
        let table_key = format!("node:{}", type_name);
        let ds = snapshot.open(&table_key).await?;

        // Scan for the row with matching id, requesting _rowid
        let filter_sql = format!("id = '{}'", id.replace('\'', "''"));
        let mut scanner = ds.scan();
        scanner.with_row_id();
        scanner
            .project(&["id"])
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        scanner
            .filter(&filter_sql)
            .map_err(|e| OmniError::Lance(format!("blob filter: {}", e)))?;

        let batches: Vec<arrow_array::RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        // Extract the row ID
        let row_id = batches
            .iter()
            .find_map(|batch| {
                batch
                    .column_by_name("_rowid")
                    .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
                    .and_then(|arr| {
                        if arr.len() > 0 {
                            Some(arr.value(0))
                        } else {
                            None
                        }
                    })
            })
            .ok_or_else(|| {
                OmniError::Manifest(format!("no {} with id '{}' found", type_name, id))
            })?;

        // Use take_blobs to get the BlobFile handle
        let ds = Arc::new(ds);
        let mut blobs = ds
            .take_blobs(&[row_id], property)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;

        blobs.pop().ok_or_else(|| {
            OmniError::Manifest(format!(
                "blob '{}' on {} '{}' returned no data",
                property, type_name, id
            ))
        })
    }

    pub fn active_branch(&self) -> Option<&str> {
        self.coordinator.current_branch()
    }

    pub(crate) fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
        normalize_branch_name(branch)
    }

    pub(crate) async fn head_commit_id(&self) -> Result<Option<String>> {
        self.coordinator
            .head_commit_id()
            .await
            .map(|id| id.map(|snapshot_id| snapshot_id.as_str().to_string()))
    }

    pub async fn branch_create(&mut self, name: &str) -> Result<()> {
        self.coordinator.branch_create(name).await
    }

    pub async fn branch_list(&self) -> Result<Vec<String>> {
        self.coordinator.branch_list().await
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
        let snapshot = self.snapshot();
        let entry = snapshot
            .entry(table_key)
            .ok_or_else(|| OmniError::Manifest(format!("no manifest entry for {}", table_key)))?;
        let full_path = format!("{}/{}", self.root_uri, entry.table_path);
        match self.coordinator.current_branch() {
            None => {
                let ds = self.open_dataset_head(&full_path, None).await?;
                ensure_expected_version(&ds, table_key, entry.table_version)?;
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
                ensure_expected_version(&ds, table_key, entry_version)?;
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
        let mut source_ds = self
            .open_dataset_head(full_path, source_branch)
            .await?
            .checkout_version(source_version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        ensure_expected_version(&source_ds, table_key, source_version)?;

        match source_ds
            .create_branch(active_branch, source_version, None)
            .await
        {
            Ok(_) => {}
            Err(create_err) => {
                return self
                    .open_existing_branch_after_create_race(
                        table_key,
                        full_path,
                        active_branch,
                        source_version,
                        create_err.to_string(),
                    )
                    .await;
            }
        }

        let ds = self
            .open_dataset_head(full_path, Some(active_branch))
            .await?;
        ensure_expected_version(&ds, table_key, source_version)?;
        Ok(ds)
    }

    async fn open_existing_branch_after_create_race(
        &self,
        table_key: &str,
        full_path: &str,
        active_branch: &str,
        expected_version: u64,
        create_err: String,
    ) -> Result<Dataset> {
        match self.open_dataset_head(full_path, Some(active_branch)).await {
            Ok(ds) => {
                ensure_expected_version(&ds, table_key, expected_version)?;
                Ok(ds)
            }
            Err(_) => Err(OmniError::Lance(create_err)),
        }
    }

    pub(crate) async fn reopen_for_mutation(
        &self,
        table_key: &str,
        full_path: &str,
        table_branch: Option<&str>,
        expected_version: u64,
    ) -> Result<Dataset> {
        let ds = self.open_dataset_head(full_path, table_branch).await?;
        ensure_expected_version(&ds, table_key, expected_version)?;
        Ok(ds)
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
        let params = ScalarIndexParams::default();

        if let Some(type_name) = table_key.strip_prefix("node:") {
            ds.create_index_builder(&["id"], IndexType::BTree, &params)
                .replace(true)
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
                            let inverted_params = InvertedIndexParams::default();
                            ds.create_index_builder(
                                &[prop_name.as_str()],
                                IndexType::Inverted,
                                &inverted_params,
                            )
                            .replace(true)
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
                            let vector_params = VectorIndexParams::ivf_flat(1, MetricType::L2);
                            ds.create_index_builder(
                                &[prop_name.as_str()],
                                IndexType::Vector,
                                &vector_params,
                            )
                            .replace(true)
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
            ds.create_index_builder(&["id"], IndexType::BTree, &params)
                .replace(true)
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(id): {}", table_key, e))
                })?;
            ds.create_index_builder(&["src"], IndexType::BTree, &params)
                .replace(true)
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(src): {}", table_key, e))
                })?;
            ds.create_index_builder(&["dst"], IndexType::BTree, &params)
                .replace(true)
                .await
                .map_err(|e| {
                    OmniError::Lance(format!("create BTree index on {}(dst): {}", table_key, e))
                })?;
            return Ok(());
        }

        Err(OmniError::Manifest(format!(
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

    pub(crate) async fn ensure_commit_graph_initialized(&mut self) -> Result<()> {
        self.coordinator.ensure_commit_graph_initialized().await
    }

    /// Invalidate the cached graph index. Called after edge mutations.
    pub(crate) fn invalidate_graph_index(&mut self) {
        self.runtime_cache.invalidate_all();
    }
}

pub(crate) fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
    let branch = branch.trim();
    if branch.is_empty() {
        return Err(OmniError::Manifest(
            "branch name cannot be empty".to_string(),
        ));
    }
    if branch == "main" {
        return Ok(None);
    }
    Ok(Some(branch.to_string()))
}

fn ensure_expected_version(ds: &Dataset, table_key: &str, expected_version: u64) -> Result<()> {
    if ds.version().version != expected_version {
        return Err(OmniError::Manifest(format!(
            "version drift on {}: snapshot pinned v{} but dataset is at v{} — call refresh() and retry",
            table_key,
            expected_version,
            ds.version().version
        )));
    }
    Ok(())
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

    impl StorageAdapter for RecordingStorageAdapter {
        fn read_text(&self, uri: &str) -> Result<String> {
            self.reads.lock().unwrap().push(uri.to_string());
            self.inner.read_text(uri)
        }

        fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
            self.writes.lock().unwrap().push(uri.to_string());
            self.inner.write_text(uri, contents)
        }

        fn exists(&self, uri: &str) -> Result<bool> {
            self.exists_checks.lock().unwrap().push(uri.to_string());
            self.inner.exists(uri)
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
