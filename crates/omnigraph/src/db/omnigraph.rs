use std::collections::{BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;

use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
    Int32Array, Int64Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray,
    RecordBatch, StringArray, StructArray, UInt32Array, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Field, Schema};
use lance::Dataset;
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::BlobFile;
use lance::dataset::scanner::ColumnOrdering;
use lance::datatypes::BlobKind;
use omnigraph_compiler::catalog::{Catalog, EdgeType, NodeType};
use omnigraph_compiler::schema::parser::parse_schema;
use omnigraph_compiler::types::ScalarType;
use omnigraph_compiler::{
    SchemaIR, SchemaMigrationPlan, SchemaMigrationStep, SchemaTypeKind, build_catalog_from_ir,
    build_schema_ir, plan_schema_migration,
};

use crate::db::graph_coordinator::{GraphCoordinator, PublishedSnapshot};
use crate::error::{OmniError, Result};
use crate::runtime_cache::RuntimeCache;
use crate::storage::{StorageAdapter, join_uri, normalize_root_uri, storage_for_uri};
use crate::table_store::TableStore;

mod export;
mod optimize;
mod schema_apply;
mod table_ops;

pub use optimize::{CleanupPolicyOptions, TableCleanupStats, TableOptimizeStats};

use super::commit_graph::GraphCommit;
use super::manifest::{
    ManifestChange, Snapshot, SubTableEntry, TableRegistration, TableTombstone,
    table_path_for_table_key,
};
use super::schema_state::{
    SCHEMA_SOURCE_FILENAME, load_or_bootstrap_schema_contract, read_accepted_schema_ir,
    recover_schema_state_files, schema_ir_staging_uri, schema_ir_uri, schema_source_staging_uri,
    schema_source_uri, schema_state_staging_uri, schema_state_uri, validate_schema_contract,
    write_schema_contract, write_schema_contract_staging,
};
use super::{
    ReadTarget, ResolvedTarget, SCHEMA_APPLY_LOCK_BRANCH, SnapshotId, is_internal_system_branch,
    is_schema_apply_lock_branch,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeOutcome {
    AlreadyUpToDate,
    FastForward,
    Merged,
}

#[derive(Debug, Clone)]
pub struct SchemaApplyResult {
    pub supported: bool,
    pub applied: bool,
    pub manifest_version: u64,
    pub steps: Vec<SchemaMigrationStep>,
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
    pub(crate) audit_actor_id: Option<String>,
}

/// Whether [`Omnigraph::open`] runs the open-time recovery sweep.
///
/// Recovery requires Lance writes (`Dataset::restore`, `ManifestBatchPublisher::publish`).
/// Read-only consumers — NDJSON export, `commit list`, `read`, schema
/// inspection — should not trigger writes (they may run with read-only
/// object-store credentials, and silent open-time mutations are
/// surprising). They also don't need recovery: reads always resolve
/// through the manifest pin, which is the consistent snapshot regardless
/// of any Phase B → Phase C drift on the per-table side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Run the recovery sweep on open. Default for `Omnigraph::open`.
    ReadWrite,
    /// Skip the recovery sweep. Use for read-only consumers via
    /// [`Omnigraph::open_read_only`].
    ReadOnly,
}

impl Omnigraph {
    /// Create a new repo at `uri` from schema source.
    ///
    /// Creates `_schema.pg`, per-type Lance datasets, and `__manifest`.
    pub async fn init(uri: &str, schema_source: &str) -> Result<Self> {
        Self::init_with_storage(uri, schema_source, storage_for_uri(uri)?).await
    }

    pub(crate) async fn init_with_storage(
        uri: &str,
        schema_source: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri)?;
        let schema_ir = read_schema_ir_from_source(schema_source)?;
        let mut catalog = build_catalog_from_ir(&schema_ir)?;
        fixup_blob_schemas(&mut catalog);

        // Write _schema.pg
        let schema_path = join_uri(&root, SCHEMA_SOURCE_FILENAME);
        storage.write_text(&schema_path, schema_source).await?;
        write_schema_contract(&root, storage.as_ref(), &schema_ir).await?;

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
            audit_actor_id: None,
        })
    }

    /// Open an existing repo (read-write).
    ///
    /// Reads `_schema.pg`, parses it, builds the catalog, and opens `__manifest`.
    /// Runs the open-time recovery sweep before returning — see [`OpenMode`].
    pub async fn open(uri: &str) -> Result<Self> {
        Self::open_with_storage_and_mode(uri, storage_for_uri(uri)?, OpenMode::ReadWrite).await
    }

    /// Open an existing repo for read-only consumers (NDJSON export,
    /// `commit list`, etc.). Skips the recovery sweep — see [`OpenMode`].
    pub async fn open_read_only(uri: &str) -> Result<Self> {
        Self::open_with_storage_and_mode(uri, storage_for_uri(uri)?, OpenMode::ReadOnly).await
    }

    /// `open_with_storage` retained for existing callers (init/test paths).
    /// Defaults to `OpenMode::ReadWrite`.
    pub(crate) async fn open_with_storage(
        uri: &str,
        storage: Arc<dyn StorageAdapter>,
    ) -> Result<Self> {
        Self::open_with_storage_and_mode(uri, storage, OpenMode::ReadWrite).await
    }

    pub(crate) async fn open_with_storage_and_mode(
        uri: &str,
        storage: Arc<dyn StorageAdapter>,
        mode: OpenMode,
    ) -> Result<Self> {
        let root = normalize_root_uri(uri)?;
        // Open the coordinator first so the schema-staging recovery sweep can
        // compare its snapshot against any leftover staging files.
        let mut coordinator = GraphCoordinator::open(&root, Arc::clone(&storage)).await?;
        // Both the schema-state recovery sweep AND the manifest-drift
        // recovery sweep are gated on `OpenMode::ReadWrite`. Read-only
        // consumers (NDJSON export, `commit list`, schema show) shouldn't
        // trigger object-store mutations: they may run with read-only
        // credentials, and silent open-time writes are surprising. Both
        // sweeps' work is recoverable on the next ReadWrite open, so
        // skipping under ReadOnly doesn't lose any safety guarantees —
        // the manifest pin is the consistent snapshot regardless of
        // drift on the per-table side or leftover schema-staging files.
        if matches!(mode, OpenMode::ReadWrite) {
            recover_schema_state_files(&root, Arc::clone(&storage), &coordinator.snapshot())
                .await?;
            // Recovery sweep: close the Phase B → Phase C residual on
            // any sidecar left over from a crashed writer. Continuous
            // in-process recovery for long-running servers (no restart
            // required between Phase B failure and recovery) is a
            // separate background-reconciler effort.
            crate::db::manifest::recover_manifest_drift(
                &root,
                Arc::clone(&storage),
                &mut coordinator,
                crate::db::manifest::RecoveryMode::Full,
            )
            .await?;
        }
        // Read _schema.pg (post-recovery — may have just been renamed in).
        let schema_path = schema_source_uri(&root);
        let schema_source = storage.read_text(&schema_path).await?;
        let current_source_ir = read_schema_ir_from_source(&schema_source)?;
        let branches = coordinator.branch_list().await?;
        let (accepted_ir, _) = load_or_bootstrap_schema_contract(
            &root,
            Arc::clone(&storage),
            &branches,
            &current_source_ir,
        )
        .await?;
        let mut catalog = build_catalog_from_ir(&accepted_ir)?;
        fixup_blob_schemas(&mut catalog);

        Ok(Self {
            root_uri: root.clone(),
            storage,
            coordinator,
            table_store: TableStore::new(&root),
            runtime_cache: RuntimeCache::default(),
            catalog,
            schema_source,
            audit_actor_id: None,
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

    pub(crate) async fn ensure_schema_state_valid(&self) -> Result<()> {
        validate_schema_contract(self.uri(), Arc::clone(&self.storage)).await
    }

    pub async fn plan_schema(&self, desired_schema_source: &str) -> Result<SchemaMigrationPlan> {
        schema_apply::plan_schema(self, desired_schema_source).await
    }

    pub async fn apply_schema(&mut self, desired_schema_source: &str) -> Result<SchemaApplyResult> {
        schema_apply::apply_schema(self, desired_schema_source).await
    }

    pub(crate) async fn ensure_schema_apply_idle(&mut self, operation: &str) -> Result<()> {
        schema_apply::ensure_schema_apply_idle(self, operation).await
    }

    async fn ensure_schema_apply_not_locked(&self, operation: &str) -> Result<()> {
        schema_apply::ensure_schema_apply_not_locked(self, operation).await
    }

    pub(crate) fn table_store(&self) -> &TableStore {
        &self.table_store
    }

    /// Engine-facing trait surface around `TableStore`.
    ///
    /// This is the canonical accessor for newly-written engine code. The
    /// trait's signatures use opaque `SnapshotHandle` / `StagedHandle`
    /// instead of leaking `lance::Dataset` /
    /// `lance::dataset::transaction::Transaction`. Existing call sites
    /// that still use `db.table_store.X(...)` (the inherent struct
    /// methods) are migrated incrementally.
    pub(crate) fn storage(&self) -> &dyn crate::storage_layer::TableStorage {
        &self.table_store
    }

    /// Engine-level access to the object-store adapter (S3 / local fs).
    /// Used by the recovery sidecar protocol — writers in the engine
    /// call this to write/delete sidecars at `__recovery/{ulid}.json`.
    pub(crate) fn storage_adapter(&self) -> &dyn crate::storage::StorageAdapter {
        self.storage.as_ref()
    }

    /// Engine-level access to the repo's normalized root URI. Used by
    /// the recovery sidecar protocol to compute `__recovery/` paths.
    pub(crate) fn root_uri(&self) -> &str {
        &self.root_uri
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
        self.ensure_schema_state_valid().await?;
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

    pub async fn resolved_branch_of(
        &self,
        target: impl Into<ReadTarget>,
    ) -> Result<Option<String>> {
        self.resolved_target(target)
            .await
            .map(|resolved| resolved.branch)
    }

    /// Synchronize this handle's write base to the latest head of the named branch.
    pub async fn sync_branch(&mut self, branch: &str) -> Result<()> {
        self.ensure_schema_state_valid().await?;
        let branch = normalize_branch_name(branch)?;
        self.coordinator = self.open_coordinator_for_branch(branch.as_deref()).await?;
        self.runtime_cache.invalidate_all().await;
        Ok(())
    }

    /// Re-read the handle-local coordinator state from storage AND run
    /// roll-forward-only recovery: closes the in-process Phase B → Phase C
    /// residual (e.g. `MutationStaging::finalize` crash mid-publish in a
    /// long-running server) without restart. Roll-forward uses
    /// `ManifestBatchPublisher::publish`'s row-level CAS — safe under
    /// concurrency. Sidecars that would require `Dataset::restore` are
    /// deferred to the next ReadWrite open (restore can silently orphan
    /// a concurrent writer's commit if invoked under concurrency).
    ///
    /// Steady state cost: one `list_dir` of `__recovery/` (typically
    /// returns empty → early return). No additional Lance reads.
    ///
    /// Engine-internal callers that already hold an in-flight sidecar
    /// (e.g. `schema_apply` mid-write) MUST use
    /// [`refresh_coordinator_only`](Self::refresh_coordinator_only) to
    /// avoid the recovery sweep racing their own sidecar.
    pub async fn refresh(&mut self) -> Result<()> {
        self.coordinator.refresh().await?;
        crate::db::manifest::recover_manifest_drift(
            &self.root_uri,
            Arc::clone(&self.storage),
            &mut self.coordinator,
            crate::db::manifest::RecoveryMode::RollForwardOnly,
        )
        .await?;
        self.runtime_cache.invalidate_all().await;
        Ok(())
    }

    /// Refresh coordinator state and invalidate the runtime cache WITHOUT
    /// running the recovery sweep. Engine-internal callers that hold an
    /// in-flight sidecar (e.g. `schema_apply::apply_schema_with_lock`'s
    /// internal lease-check refresh) need this variant: running recovery
    /// here would observe the caller's own sidecar, classify it as
    /// RolledPastExpected, and roll it forward — racing the caller's
    /// own publish path.
    pub(crate) async fn refresh_coordinator_only(&mut self) -> Result<()> {
        self.coordinator.refresh().await?;
        self.runtime_cache.invalidate_all().await;
        Ok(())
    }

    pub async fn resolve_snapshot(&self, branch: &str) -> Result<SnapshotId> {
        self.ensure_schema_state_valid().await?;
        self.coordinator.resolve_snapshot_id(branch).await
    }

    pub(crate) async fn resolved_target(
        &self,
        target: impl Into<ReadTarget>,
    ) -> Result<ResolvedTarget> {
        self.ensure_schema_state_valid().await?;
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
        export::entity_at_target(self, target, table_key, id).await
    }

    /// Read one entity at a specific manifest version via time travel (on-demand enrichment).
    pub async fn entity_at(
        &self,
        table_key: &str,
        id: &str,
        version: u64,
    ) -> Result<Option<serde_json::Value>> {
        export::entity_at(self, table_key, id, version).await
    }

    /// Create a Snapshot at any historical manifest version.
    pub async fn snapshot_at_version(&self, version: u64) -> Result<Snapshot> {
        self.ensure_schema_state_valid().await?;
        self.coordinator.snapshot_at_version(version).await
    }

    pub async fn export_jsonl(
        &self,
        branch: &str,
        type_names: &[String],
        table_keys: &[String],
    ) -> Result<String> {
        export::export_jsonl(self, branch, type_names, table_keys).await
    }

    pub async fn export_jsonl_to_writer<W: Write>(
        &self,
        branch: &str,
        type_names: &[String],
        table_keys: &[String],
        writer: &mut W,
    ) -> Result<()> {
        export::export_jsonl_to_writer(self, branch, type_names, table_keys, writer).await
    }

    // ─── Graph index ──────────────────────────────────────────────────────

    /// Get or build the graph index for the current snapshot.
    pub async fn graph_index(&self) -> Result<Arc<crate::graph_index::GraphIndex>> {
        table_ops::graph_index(self).await
    }

    pub(crate) async fn graph_index_for_resolved(
        &self,
        resolved: &ResolvedTarget,
    ) -> Result<Arc<crate::graph_index::GraphIndex>> {
        table_ops::graph_index_for_resolved(self, resolved).await
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
        table_ops::ensure_indices(self).await
    }

    pub async fn ensure_indices_on(&mut self, branch: &str) -> Result<()> {
        table_ops::ensure_indices_on(self, branch).await
    }

    /// Compact small Lance fragments into fewer larger ones across every
    /// node + edge table on `main`. See [`optimize`] for details.
    pub async fn optimize(&mut self) -> Result<Vec<optimize::TableOptimizeStats>> {
        optimize::optimize_all_tables(self).await
    }

    /// Remove Lance manifests (and the fragments they uniquely own) per the
    /// given [`optimize::CleanupPolicyOptions`]. Destructive to version
    /// history. See [`optimize`] for details.
    pub async fn cleanup(
        &mut self,
        options: optimize::CleanupPolicyOptions,
    ) -> Result<Vec<optimize::TableCleanupStats>> {
        optimize::cleanup_all_tables(self, options).await
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
        self.ensure_schema_state_valid().await?;
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

    async fn ensure_branch_delete_safe(&self, branch: &str, branches: &[String]) -> Result<()> {
        let descendants = self.coordinator.branch_descendants(branch).await?;
        if let Some(descendant) = descendants.first() {
            return Err(OmniError::manifest_conflict(format!(
                "cannot delete branch '{}' because descendant branch '{}' still depends on it",
                branch, descendant
            )));
        }

        for other_branch in branches
            .iter()
            .filter(|candidate| candidate.as_str() != branch)
        {
            let snapshot = self
                .snapshot_of(ReadTarget::branch(other_branch.as_str()))
                .await?;
            if snapshot
                .entries()
                .any(|entry| entry.table_branch.as_deref() == Some(branch))
            {
                return Err(OmniError::manifest_conflict(format!(
                    "cannot delete branch '{}' because branch '{}' still depends on it",
                    branch, other_branch
                )));
            }
        }

        Ok(())
    }

    async fn cleanup_deleted_branch_tables(
        &self,
        branch: &str,
        owned_tables: &[(String, String)],
    ) -> Result<()> {
        let mut seen_paths = HashSet::new();
        let mut cleanup_targets = owned_tables
            .iter()
            .filter(|(_, table_path)| seen_paths.insert(table_path.clone()))
            .cloned()
            .collect::<Vec<_>>();
        cleanup_targets.sort_by(|left, right| left.0.cmp(&right.0));

        for (table_key, table_path) in cleanup_targets {
            let dataset_uri = self.table_store.dataset_uri(&table_path);
            if let Err(err) = self.table_store.delete_branch(&dataset_uri, branch).await {
                return Err(OmniError::manifest_internal(format!(
                    "branch '{}' was deleted but cleanup failed for {}: {}",
                    branch, table_key, err
                )));
            }
        }

        Ok(())
    }

    async fn delete_branch_storage_only(&mut self, branch: &str) -> Result<()> {
        if self.coordinator.current_branch() == Some(branch) {
            return Err(OmniError::manifest_conflict(format!(
                "cannot delete currently active branch '{}'",
                branch
            )));
        }

        let branch_snapshot = self.snapshot_of(ReadTarget::branch(branch)).await?;
        let owned_tables = branch_snapshot
            .entries()
            .filter(|entry| entry.table_branch.as_deref() == Some(branch))
            .map(|entry| (entry.table_key.clone(), entry.table_path.clone()))
            .collect::<Vec<_>>();

        self.coordinator.branch_delete(branch).await?;
        self.cleanup_deleted_branch_tables(branch, &owned_tables)
            .await
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
        self.ensure_schema_state_valid().await?;
        self.ensure_schema_apply_idle("branch_create").await?;
        ensure_public_branch_ref(name, "branch_create")?;
        self.coordinator.branch_create(name).await
    }

    pub(crate) fn current_audit_actor(&self) -> Option<&str> {
        self.audit_actor_id.as_deref()
    }

    pub async fn branch_create_from(
        &mut self,
        from: impl Into<ReadTarget>,
        name: &str,
    ) -> Result<()> {
        self.ensure_schema_apply_idle("branch_create_from").await?;
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
        self.ensure_schema_state_valid().await?;
        self.coordinator.branch_list().await
    }

    pub async fn branch_delete(&mut self, name: &str) -> Result<()> {
        self.ensure_schema_state_valid().await?;
        self.ensure_schema_apply_idle("branch_delete").await?;
        ensure_public_branch_ref(name, "branch_delete")?;
        self.refresh().await?;
        let branch = normalize_branch_name(name)?
            .ok_or_else(|| OmniError::manifest("cannot delete branch 'main'".to_string()))?;
        let branches = self.coordinator.branch_list().await?;
        if !branches.iter().any(|candidate| candidate == &branch) {
            return Err(OmniError::manifest_not_found(format!(
                "branch '{}' not found",
                branch
            )));
        }

        self.ensure_branch_delete_safe(&branch, &branches).await?;
        self.delete_branch_storage_only(&branch).await
    }

    pub async fn get_commit(&self, commit_id: &str) -> Result<GraphCommit> {
        self.ensure_schema_state_valid().await?;
        self.coordinator
            .resolve_commit(&SnapshotId::new(commit_id))
            .await
    }

    pub async fn list_commits(&self, branch: Option<&str>) -> Result<Vec<GraphCommit>> {
        self.ensure_schema_state_valid().await?;
        let branch = match branch {
            Some(branch) => normalize_branch_name(branch)?,
            None => None,
        };
        let coordinator = self.open_coordinator_for_branch(branch.as_deref()).await?;
        coordinator.list_commits().await
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
        table_ops::open_for_mutation(self, table_key).await
    }

    pub(crate) async fn open_for_mutation_on_branch(
        &self,
        branch: Option<&str>,
        table_key: &str,
    ) -> Result<(Dataset, String, Option<String>)> {
        table_ops::open_for_mutation_on_branch(self, branch, table_key).await
    }

    pub(crate) async fn fork_dataset_from_entry_state(
        &self,
        table_key: &str,
        full_path: &str,
        source_branch: Option<&str>,
        source_version: u64,
        active_branch: &str,
    ) -> Result<Dataset> {
        table_ops::fork_dataset_from_entry_state(
            self,
            table_key,
            full_path,
            source_branch,
            source_version,
            active_branch,
        )
        .await
    }

    pub(crate) async fn reopen_for_mutation(
        &self,
        table_key: &str,
        full_path: &str,
        table_branch: Option<&str>,
        expected_version: u64,
    ) -> Result<Dataset> {
        table_ops::reopen_for_mutation(self, table_key, full_path, table_branch, expected_version)
            .await
    }

    pub(crate) async fn open_dataset_at_state(
        &self,
        table_path: &str,
        table_branch: Option<&str>,
        table_version: u64,
    ) -> Result<Dataset> {
        table_ops::open_dataset_at_state(self, table_path, table_branch, table_version).await
    }

    pub(crate) async fn build_indices_on_dataset(
        &self,
        table_key: &str,
        ds: &mut Dataset,
    ) -> Result<()> {
        table_ops::build_indices_on_dataset(self, table_key, ds).await
    }

    pub(crate) async fn build_indices_on_dataset_for_catalog(
        &self,
        catalog: &Catalog,
        table_key: &str,
        ds: &mut Dataset,
    ) -> Result<()> {
        table_ops::build_indices_on_dataset_for_catalog(self, catalog, table_key, ds).await
    }

    // Used only by in-tree tests (`#[cfg(test)]`); the runtime path now
    // uses `commit_updates_on_branch_with_expected` exclusively.
    #[cfg(test)]
    pub(crate) async fn commit_updates(
        &mut self,
        updates: &[crate::db::SubTableUpdate],
    ) -> Result<u64> {
        table_ops::commit_updates(self, updates).await
    }

    pub(crate) async fn commit_manifest_updates(
        &mut self,
        updates: &[crate::db::SubTableUpdate],
    ) -> Result<u64> {
        table_ops::commit_manifest_updates(self, updates).await
    }

    pub(crate) async fn record_merge_commit(
        &mut self,
        manifest_version: u64,
        parent_commit_id: &str,
        merged_parent_commit_id: &str,
    ) -> Result<String> {
        table_ops::record_merge_commit(
            self,
            manifest_version,
            parent_commit_id,
            merged_parent_commit_id,
        )
        .await
    }

    pub(crate) async fn commit_updates_on_branch_with_expected(
        &mut self,
        branch: Option<&str>,
        updates: &[crate::db::SubTableUpdate],
        expected_table_versions: &std::collections::HashMap<String, u64>,
    ) -> Result<u64> {
        table_ops::commit_updates_on_branch_with_expected(
            self,
            branch,
            updates,
            expected_table_versions,
        )
        .await
    }

    pub(crate) async fn ensure_commit_graph_initialized(&mut self) -> Result<()> {
        table_ops::ensure_commit_graph_initialized(self).await
    }

    /// Invalidate the cached graph index. Called after edge mutations.
    pub(crate) async fn invalidate_graph_index(&self) {
        table_ops::invalidate_graph_index(self).await
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

pub(crate) fn ensure_public_branch_ref(branch: &str, operation: &str) -> Result<()> {
    if super::is_internal_run_branch(branch) {
        return Err(OmniError::manifest(format!(
            "{} does not allow internal run ref '{}'",
            operation, branch
        )));
    }
    if is_internal_system_branch(branch) {
        return Err(OmniError::manifest(format!(
            "{} does not allow internal system ref '{}'",
            operation, branch
        )));
    }
    Ok(())
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

fn read_schema_ir_from_source(schema_source: &str) -> Result<SchemaIR> {
    let schema_ast = parse_schema(schema_source)?;
    build_schema_ir(&schema_ast).map_err(|err| OmniError::manifest(err.to_string()))
}

fn schema_table_key(type_kind: SchemaTypeKind, name: &str) -> String {
    match type_kind {
        SchemaTypeKind::Node => format!("node:{}", name),
        SchemaTypeKind::Edge => format!("edge:{}", name),
        SchemaTypeKind::Interface => unreachable!("interfaces do not map to tables"),
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
    use crate::db::is_internal_run_branch;
    use crate::db::manifest::ManifestCoordinator;
    use async_trait::async_trait;
    use serde_json::Value;
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
        renames: Mutex<Vec<(String, String)>>,
        deletes: Mutex<Vec<String>>,
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

        async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
            self.renames
                .lock()
                .unwrap()
                .push((from_uri.to_string(), to_uri.to_string()));
            self.inner.rename_text(from_uri, to_uri).await
        }

        async fn delete(&self, uri: &str) -> Result<()> {
            self.deletes.lock().unwrap().push(uri.to_string());
            self.inner.delete(uri).await
        }

        async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
            self.inner.list_dir(dir_uri).await
        }
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
        assert!(adapter.writes().contains(&join_uri(uri, "_schema.ir.json")));
        assert!(
            adapter
                .writes()
                .contains(&join_uri(uri, "__schema_state.json"))
        );

        Omnigraph::open_with_storage(uri, adapter.clone())
            .await
            .unwrap();
        assert!(adapter.reads().contains(&join_uri(uri, "_schema.pg")));
        assert!(adapter.reads().contains(&join_uri(uri, "_schema.ir.json")));
        assert!(
            adapter
                .reads()
                .contains(&join_uri(uri, "__schema_state.json"))
        );
        assert!(
            adapter
                .exists_checks()
                .contains(&join_uri(uri, "_schema.ir.json"))
        );
        assert!(
            adapter
                .exists_checks()
                .contains(&join_uri(uri, "__schema_state.json"))
        );
        assert!(
            adapter
                .exists_checks()
                .contains(&join_uri(uri, "_graph_commits.lance"))
        );
    }

    async fn table_rows_json(db: &Omnigraph, table_key: &str) -> Vec<Value> {
        let snapshot = db.snapshot();
        let ds = snapshot.open(table_key).await.unwrap();
        let batches = db.table_store().scan_batches(&ds).await.unwrap();
        batches
            .into_iter()
            .flat_map(|batch| {
                (0..batch.num_rows())
                    .map(|row| record_batch_row_to_json(&batch, row).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    async fn seed_person_row(db: &mut Omnigraph, name: &str, age: Option<i32>) {
        let (mut ds, full_path, table_branch) = db.open_for_mutation("node:Person").await.unwrap();
        let schema: Arc<Schema> = Arc::new(ds.schema().into());
        let columns: Vec<Arc<dyn Array>> = schema
            .fields()
            .iter()
            .map(|field| match field.name().as_str() {
                "id" => Arc::new(StringArray::from(vec![name])) as Arc<dyn Array>,
                "name" => Arc::new(StringArray::from(vec![name])) as Arc<dyn Array>,
                "age" => Arc::new(Int32Array::from(vec![age])) as Arc<dyn Array>,
                _ => new_null_array(field.data_type(), 1),
            })
            .collect();
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let state = db
            .table_store()
            .append_batch(&full_path, &mut ds, batch)
            .await
            .unwrap();
        db.commit_updates(&[crate::db::SubTableUpdate {
            table_key: "node:Person".to_string(),
            table_version: state.version,
            table_branch,
            row_count: state.row_count,
            version_metadata: state.version_metadata,
        }])
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_apply_schema_adds_nullable_property_and_preserves_rows() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        seed_person_row(&mut db, "Alice", Some(30)).await;

        let desired = TEST_SCHEMA.replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        );
        let result = db.apply_schema(&desired).await.unwrap();
        assert!(result.applied);

        let reopened = Omnigraph::open(uri).await.unwrap();
        let rows = table_rows_json(&reopened, "node:Person").await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[0]["age"], 30);
        assert!(rows[0]["nickname"].is_null());
        assert!(
            reopened.catalog().node_types["Person"]
                .properties
                .contains_key("nickname")
        );
        assert!(dir.path().join("_schema.pg").exists());
    }

    #[tokio::test]
    async fn test_apply_schema_renames_property_and_preserves_values() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        seed_person_row(&mut db, "Alice", Some(30)).await;

        let desired = TEST_SCHEMA.replace(
            "    age: I32?\n}",
            "    years: I32? @rename_from(\"age\")\n}",
        );
        db.apply_schema(&desired).await.unwrap();

        let reopened = Omnigraph::open(uri).await.unwrap();
        let rows = table_rows_json(&reopened, "node:Person").await;
        assert_eq!(rows[0]["name"], "Alice");
        assert_eq!(rows[0]["years"], 30);
        assert!(rows[0].get("age").is_none());
    }

    #[tokio::test]
    async fn test_apply_schema_renames_type_and_preserves_historical_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        seed_person_row(&mut db, "Alice", Some(30)).await;
        let before_version = db.snapshot().version();

        let desired = TEST_SCHEMA
            .replace("node Person {\n", "node Human @rename_from(\"Person\") {\n")
            .replace("edge Knows: Person -> Person", "edge Knows: Human -> Human")
            .replace(
                "edge WorksAt: Person -> Company",
                "edge WorksAt: Human -> Company",
            );
        db.apply_schema(&desired).await.unwrap();

        let head = db.snapshot();
        assert!(head.entry("node:Person").is_none());
        assert!(head.entry("node:Human").is_some());
        let historical = ManifestCoordinator::snapshot_at(uri, None, before_version)
            .await
            .unwrap();
        assert!(historical.entry("node:Person").is_some());
        assert!(historical.entry("node:Human").is_none());
    }

    #[tokio::test]
    async fn test_apply_schema_succeeds_after_load() {
        // Historical: schema apply used to be blocked by leftover
        // `__run__` branches. A defense-in-depth filter now skips
        // internal system branches, and run branches were made
        // ephemeral on every terminal state — so in practice no
        // `__run__` branch survives publish. The filter still guards
        // the invariant.
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        crate::loader::load_jsonl(
            &mut db,
            r#"{"type": "Person", "data": {"name": "Alice", "age": 30}}"#,
            crate::loader::LoadMode::Overwrite,
        )
        .await
        .unwrap();

        let all_branches = db.coordinator.all_branches().await.unwrap();
        assert!(
            !all_branches.iter().any(|b| is_internal_run_branch(b)),
            "run branch should be deleted after publish, got: {:?}",
            all_branches
        );

        let desired = TEST_SCHEMA.replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        );
        let result = db.apply_schema(&desired).await.unwrap();
        assert!(result.applied, "schema apply should have applied");
    }

    #[tokio::test]
    async fn test_apply_schema_adds_index_for_existing_property() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();

        let desired = TEST_SCHEMA.replace("name: String @key", "name: String @key @index");
        db.apply_schema(&desired).await.unwrap();

        let snapshot = db.snapshot();
        let ds = snapshot.open("node:Person").await.unwrap();
        assert!(db.table_store().has_fts_index(&ds, "name").await.unwrap());
    }

    #[tokio::test]
    async fn test_apply_schema_rewrite_preserves_existing_indices() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let initial_schema = TEST_SCHEMA.replace("name: String @key", "name: String @key @index");
        let mut db = Omnigraph::init(uri, &initial_schema).await.unwrap();
        seed_person_row(&mut db, "Alice", Some(30)).await;

        let desired = initial_schema.replace(
            "    age: I32?\n}",
            "    age: I32?\n    nickname: String?\n}",
        );
        db.apply_schema(&desired).await.unwrap();

        let snapshot = db.snapshot();
        let ds = snapshot.open("node:Person").await.unwrap();
        assert!(db.table_store().has_btree_index(&ds, "id").await.unwrap());
        assert!(db.table_store().has_fts_index(&ds, "name").await.unwrap());
    }

    #[tokio::test]
    async fn test_open_for_mutation_rejects_while_schema_apply_locked() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        let mut db = db;
        db.coordinator
            .branch_create(SCHEMA_APPLY_LOCK_BRANCH)
            .await
            .unwrap();

        let err = db.open_for_mutation("node:Person").await.unwrap_err();
        assert!(
            err.to_string()
                .contains("write is unavailable while schema apply is in progress")
        );
    }

    #[tokio::test]
    async fn test_commit_updates_rejects_while_schema_apply_locked() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        db.coordinator
            .branch_create(SCHEMA_APPLY_LOCK_BRANCH)
            .await
            .unwrap();

        let err = db.commit_updates(&[]).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("write commit is unavailable while schema apply is in progress")
        );
    }

    #[tokio::test]
    async fn test_branch_list_hides_schema_apply_lock_branch() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut db = Omnigraph::init(uri, TEST_SCHEMA).await.unwrap();
        db.coordinator
            .branch_create(SCHEMA_APPLY_LOCK_BRANCH)
            .await
            .unwrap();

        let branches = db.branch_list().await.unwrap();
        assert_eq!(branches, vec!["main".to_string()]);
    }
}
