use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Array, UInt64Array};
use arrow_schema::{Field, Schema};
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

use crate::error::{OmniError, Result};
use crate::graph_index::GraphIndex;

use super::commit_graph::CommitGraph;
use super::manifest::{ManifestCoordinator, Snapshot};

const SCHEMA_FILENAME: &str = "_schema.pg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphIndexCacheKey {
    active_branch: Option<String>,
    edge_tables: Vec<GraphIndexTableState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphIndexTableState {
    table_key: String,
    table_version: u64,
    table_branch: Option<String>,
}

#[derive(Debug, Clone)]
struct CachedGraphIndex {
    key: GraphIndexCacheKey,
    index: Arc<GraphIndex>,
}

/// Top-level handle to an Omnigraph database.
///
/// An Omnigraph is a Lance-native graph database with git-style branching.
/// It stores typed property graphs as per-type Lance datasets coordinated
/// through a Lance manifest table.
pub struct Omnigraph {
    root_uri: String,
    manifest: ManifestCoordinator,
    commit_graph: Option<CommitGraph>,
    catalog: Catalog,
    schema_source: String,
    active_branch: Option<String>,
    cached_graph_index: Option<CachedGraphIndex>,
}

impl Omnigraph {
    /// Create a new repo at `uri` from schema source.
    ///
    /// Creates `_schema.pg`, per-type Lance datasets, and `_manifest.lance`.
    pub async fn init(uri: &str, schema_source: &str) -> Result<Self> {
        let root = uri.trim_end_matches('/');

        // Parse and validate schema
        let schema_ast = parse_schema(schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        // Write _schema.pg
        let schema_path = format!("{}/{}", root, SCHEMA_FILENAME);
        write_file(&schema_path, schema_source)?;

        // Create manifest + per-type datasets
        let manifest = ManifestCoordinator::init(root, &catalog).await?;
        let commit_graph = Some(CommitGraph::init(root, manifest.version()).await?);

        Ok(Self {
            root_uri: root.to_string(),
            manifest,
            commit_graph,
            catalog,
            schema_source: schema_source.to_string(),
            active_branch: None,
            cached_graph_index: None,
        })
    }

    /// Open an existing repo.
    ///
    /// Reads `_schema.pg`, parses it, builds the catalog, and opens the manifest.
    pub async fn open(uri: &str) -> Result<Self> {
        let root = uri.trim_end_matches('/');

        // Read _schema.pg
        let schema_path = format!("{}/{}", root, SCHEMA_FILENAME);
        let schema_source = read_file(&schema_path)?;

        // Parse and validate schema
        let schema_ast = parse_schema(&schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        // Open manifest
        let manifest = ManifestCoordinator::open(root).await?;
        let commit_graph = CommitGraph::open_if_exists(root).await?;

        Ok(Self {
            root_uri: root.to_string(),
            manifest,
            commit_graph,
            catalog,
            schema_source,
            active_branch: None,
            cached_graph_index: None,
        })
    }

    pub async fn open_branch(uri: &str, branch: &str) -> Result<Self> {
        let normalized = normalize_branch_name(branch)?;
        if normalized.is_none() {
            return Self::open(uri).await;
        }

        let root = uri.trim_end_matches('/');
        let schema_path = format!("{}/{}", root, SCHEMA_FILENAME);
        let schema_source = read_file(&schema_path)?;

        let schema_ast = parse_schema(&schema_source)?;
        let mut catalog = build_catalog(&schema_ast)?;
        fixup_blob_schemas(&mut catalog);

        let branch_name = normalized.clone().unwrap();
        let manifest = ManifestCoordinator::open_at_branch(root, &branch_name).await?;
        let commit_graph = match CommitGraph::open_if_exists(root).await? {
            Some(_) => Some(CommitGraph::open_at_branch(root, &branch_name).await?),
            None => {
                return Err(OmniError::Manifest(format!(
                    "branch '{}' exists in manifest but _graph_commits.lance is missing",
                    branch_name
                )));
            }
        };

        Ok(Self {
            root_uri: root.to_string(),
            manifest,
            commit_graph,
            catalog,
            schema_source,
            active_branch: normalized,
            cached_graph_index: None,
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

    pub fn version(&self) -> u64 {
        self.manifest.version()
    }

    /// Return an immutable Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        self.manifest.snapshot()
    }

    /// Re-read manifest from storage to see other writers' commits.
    pub async fn refresh(&mut self) -> Result<()> {
        self.manifest.refresh().await?;
        if let Some(commit_graph) = &mut self.commit_graph {
            commit_graph.refresh().await?;
        }

        if let Some(cache) = &self.cached_graph_index {
            let current_key = self.graph_index_cache_key();
            if cache.key != current_key {
                self.cached_graph_index = None;
            }
        }

        Ok(())
    }

    // ─── Change detection ────────────────────────────────────────────────

    /// Diff two manifest versions on the current branch.
    pub async fn diff(
        &self,
        from_version: u64,
        to_version: u64,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_snap = ManifestCoordinator::snapshot_at(
            self.uri(),
            self.active_branch.as_deref(),
            from_version,
        )
        .await?;
        let to_snap = ManifestCoordinator::snapshot_at(
            self.uri(),
            self.active_branch.as_deref(),
            to_version,
        )
        .await?;
        crate::changes::diff_snapshots(self.uri(), &from_snap, &to_snap, filter).await
    }

    /// Changes since a version on the current branch. Sugar for `diff(from, current)`.
    pub async fn changes_since(
        &self,
        from_version: u64,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_snap = ManifestCoordinator::snapshot_at(
            self.uri(),
            self.active_branch.as_deref(),
            from_version,
        )
        .await?;
        let to_snap = self.snapshot();
        crate::changes::diff_snapshots(self.uri(), &from_snap, &to_snap, filter).await
    }

    /// Diff two graph commits. Resolves each commit to `(manifest_branch, manifest_version)`
    /// and creates branch-aware snapshots. Supports cross-branch comparison.
    pub async fn diff_commits(
        &self,
        from_commit_id: &str,
        to_commit_id: &str,
        filter: &crate::changes::ChangeFilter,
    ) -> Result<crate::changes::ChangeSet> {
        let from_commit = self.resolve_commit(from_commit_id).await?;
        let to_commit = self.resolve_commit(to_commit_id).await?;
        let from_snap = ManifestCoordinator::snapshot_at(
            self.uri(),
            from_commit.manifest_branch.as_deref(),
            from_commit.manifest_version,
        )
        .await?;
        let to_snap = ManifestCoordinator::snapshot_at(
            self.uri(),
            to_commit.manifest_branch.as_deref(),
            to_commit.manifest_version,
        )
        .await?;
        crate::changes::diff_snapshots(self.uri(), &from_snap, &to_snap, filter).await
    }

    /// Read one entity at a specific manifest version via time travel (on-demand enrichment).
    pub async fn entity_at(
        &self,
        table_key: &str,
        id: &str,
        version: u64,
    ) -> Result<Option<serde_json::Value>> {
        let snap = ManifestCoordinator::snapshot_at(
            self.uri(),
            self.active_branch.as_deref(),
            version,
        )
        .await?;
        if snap.entry(table_key).is_none() {
            return Ok(None);
        }
        let ds = snap.open(table_key).await?;
        let filter_sql = format!("id = '{}'", id.replace('\'', "''"));
        let mut scanner = ds.scan();
        scanner
            .filter(&filter_sql)
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        let batches: Vec<arrow_array::RecordBatch> = scanner
            .try_into_stream()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .try_collect()
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        if batches.is_empty() || batches[0].num_rows() == 0 {
            return Ok(None);
        }
        let batch = &batches[0];
        let mut obj = serde_json::Map::new();
        for (i, field) in batch.schema().fields().iter().enumerate() {
            let val = arrow_cast::display::array_value_to_string(batch.column(i).as_ref(), 0)
                .map_err(|e| OmniError::Lance(e.to_string()))?;
            obj.insert(field.name().clone(), serde_json::Value::String(val));
        }
        Ok(Some(serde_json::Value::Object(obj)))
    }

    /// Create a Snapshot at any historical manifest version.
    pub async fn snapshot_at_version(&self, version: u64) -> Result<Snapshot> {
        ManifestCoordinator::snapshot_at(
            self.uri(),
            self.active_branch.as_deref(),
            version,
        )
        .await
    }

    async fn resolve_commit(
        &self,
        commit_id: &str,
    ) -> Result<crate::db::commit_graph::GraphCommit> {
        let cg = self.commit_graph.as_ref().ok_or_else(|| {
            OmniError::Manifest("diff_commits requires _graph_commits.lance".to_string())
        })?;
        let commits = cg.load_commits().await?;
        commits
            .into_iter()
            .find(|c| c.graph_commit_id == commit_id)
            .ok_or_else(|| {
                OmniError::Manifest(format!("commit '{}' not found", commit_id))
            })
    }

    // ─── Graph index ──────────────────────────────────────────────────────

    /// Get or build the graph index for the current snapshot.
    pub async fn graph_index(&mut self) -> Result<Arc<GraphIndex>> {
        let key = self.graph_index_cache_key();
        if let Some(cache) = &self.cached_graph_index {
            if cache.key == key {
                return Ok(Arc::clone(&cache.index));
            }
        }

        let snapshot = self.snapshot();
        let edge_types: HashMap<String, (String, String)> = self
            .catalog
            .edge_types
            .iter()
            .map(|(name, et)| (name.clone(), (et.from_type.clone(), et.to_type.clone())))
            .collect();

        let idx = Arc::new(GraphIndex::build(&snapshot, &edge_types).await?);
        self.cached_graph_index = Some(CachedGraphIndex {
            key,
            index: Arc::clone(&idx),
        });
        Ok(idx)
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
        let active_branch = self.active_branch.clone();

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

    pub fn manifest_mut(&mut self) -> &mut ManifestCoordinator {
        &mut self.manifest
    }

    pub fn active_branch(&self) -> Option<&str> {
        self.active_branch.as_deref()
    }

    pub(crate) fn normalize_branch_name(branch: &str) -> Result<Option<String>> {
        normalize_branch_name(branch)
    }

    pub(crate) async fn head_commit_id(&self) -> Result<Option<String>> {
        match &self.commit_graph {
            Some(commit_graph) => commit_graph.head_commit_id().await,
            None => Ok(None),
        }
    }

    pub async fn branch_create(&mut self, name: &str) -> Result<()> {
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::Manifest("cannot create branch 'main'".to_string()))?;
        self.ensure_commit_graph_initialized().await?;
        self.manifest.create_branch(&branch).await?;
        if let Some(commit_graph) = &mut self.commit_graph {
            commit_graph.create_branch(&branch).await?;
        }
        Ok(())
    }

    pub async fn branch_list(&self) -> Result<Vec<String>> {
        self.manifest.list_branches().await
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
        match self.active_branch.as_deref() {
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
        let ds = Dataset::open(full_path)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        match branch {
            Some(branch) => ds
                .checkout_branch(branch)
                .await
                .map_err(|e| OmniError::Lance(e.to_string())),
            None => Ok(ds),
        }
    }

    pub(crate) async fn open_dataset_at_state(
        &self,
        table_path: &str,
        table_branch: Option<&str>,
        table_version: u64,
    ) -> Result<Dataset> {
        let full_path = format!("{}/{}", self.root_uri, table_path);
        let ds = self.open_dataset_head(&full_path, table_branch).await?;
        ds.checkout_version(table_version)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
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
        let version = self.commit_manifest_updates(updates).await?;
        self.record_graph_commit(version).await?;
        Ok(version)
    }

    pub(crate) async fn commit_manifest_updates(
        &mut self,
        updates: &[crate::db::SubTableUpdate],
    ) -> Result<u64> {
        self.manifest.commit(updates).await
    }

    pub(crate) async fn record_graph_commit(&mut self, manifest_version: u64) -> Result<()> {
        if let Some(commit_graph) = &mut self.commit_graph {
            commit_graph
                .append_commit(self.active_branch.as_deref(), manifest_version)
                .await?;
        }
        Ok(())
    }

    pub(crate) async fn record_merge_commit(
        &mut self,
        manifest_version: u64,
        parent_commit_id: &str,
        merged_parent_commit_id: &str,
    ) -> Result<String> {
        let commit_graph = self.commit_graph.as_mut().ok_or_else(|| {
            OmniError::Manifest("branch merge requires _graph_commits.lance".to_string())
        })?;
        commit_graph
            .append_merge_commit(
                self.active_branch.as_deref(),
                manifest_version,
                parent_commit_id,
                merged_parent_commit_id,
            )
            .await
    }

    pub(crate) async fn ensure_commit_graph_initialized(&mut self) -> Result<()> {
        if self.commit_graph.is_some() {
            return Ok(());
        }
        CommitGraph::ensure_initialized(self.uri(), self.manifest.version()).await?;
        self.commit_graph = Some(match self.active_branch.as_deref() {
            Some(branch) => CommitGraph::open_at_branch(self.uri(), branch).await?,
            None => CommitGraph::open(self.uri()).await?,
        });
        Ok(())
    }

    /// Invalidate the cached graph index. Called after edge mutations.
    pub(crate) fn invalidate_graph_index(&mut self) {
        self.cached_graph_index = None;
    }

    fn graph_index_cache_key(&self) -> GraphIndexCacheKey {
        let snapshot = self.snapshot();
        let mut edge_tables: Vec<GraphIndexTableState> = self
            .catalog
            .edge_types
            .keys()
            .filter_map(|edge_name| {
                let table_key = format!("edge:{}", edge_name);
                snapshot
                    .entry(&table_key)
                    .map(|entry| GraphIndexTableState {
                        table_key,
                        table_version: entry.table_version,
                        table_branch: entry.table_branch.clone(),
                    })
            })
            .collect();
        edge_tables.sort_by(|a, b| a.table_key.cmp(&b.table_key));
        GraphIndexCacheKey {
            active_branch: self.active_branch.clone(),
            edge_tables,
        }
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

/// Write a file to a local path (extracted from a URI).
fn write_file(uri: &str, content: &str) -> Result<()> {
    // For now, only local paths. S3 support comes later via object_store.
    let path = Path::new(uri);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    Ok(())
}

/// Read a file from a local path (extracted from a URI).
fn read_file(uri: &str) -> Result<String> {
    let path = Path::new(uri);
    if !path.exists() {
        return Err(OmniError::Manifest(format!(
            "repo not found: {} (missing {})",
            uri, SCHEMA_FILENAME
        )));
    }
    Ok(std::fs::read_to_string(path)?)
}

#[cfg(test)]
mod tests {
    use super::*;

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
