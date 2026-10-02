//! The engine's read view types: wrappers over the catalog's, exposing only the SDK contract.

use std::sync::Arc;

use datafusion::logical_expr::Expr;
use lance::Dataset;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::datatypes::{BlobHandling, Schema as LanceSchema};
use lance_table::format::IndexMetadata;
use omnigraph_compiler::catalog::Catalog;

use crate::db::DatasetEntry;
use crate::db::schema_state::SchemaContractIdentity;
use crate::error::Result;

/// Immutable point-in-time view of the database.
///
/// Cheap to create (no storage I/O). All reads within a query go through one
/// Snapshot to guarantee cross-type consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    inner: omnigraph_catalog::Snapshot,
}

/// Read-only view of one backing dataset pinned by a [`Snapshot`].
///
/// The underlying Lance [`Dataset`] is deliberately private: a snapshot dataset
/// can scan rows and inspect read metadata, but it cannot reach Lance's
/// mutating APIs or advance a dataset HEAD outside OmniGraph's coordinated write
/// path.
#[derive(Debug, Clone)]
pub struct SnapshotDataset {
    inner: omnigraph_catalog::SnapshotDataset,
}

/// Read-only scan builder for a [`SnapshotDataset`].
///
/// This forwards scan configuration and execution, but not Lance's raw
/// [`Scanner`](lance::dataset::scanner::Scanner) or physical-plan construction. A Lance physical scan plan
/// exposes its embedded [`Dataset`], which would let SDK callers recover a
/// writable handle and bypass graph publication.
pub struct SnapshotScanner {
    inner: omnigraph_catalog::SnapshotScanner,
}

impl SnapshotScanner {
    pub(crate) fn wrap(inner: omnigraph_catalog::SnapshotScanner) -> Self {
        Self { inner }
    }

    /// Select the output columns.
    pub fn project<T: AsRef<str>>(&mut self, columns: &[T]) -> Result<&mut Self> {
        self.inner.project(columns)?;
        Ok(self)
    }

    /// Apply a SQL filter expression.
    pub fn filter(&mut self, filter: &str) -> Result<&mut Self> {
        self.inner.filter(filter)?;
        Ok(self)
    }

    /// Apply a structured DataFusion filter expression.
    pub fn filter_expr(&mut self, filter: Expr) -> &mut Self {
        self.inner.filter_expr(filter);
        self
    }

    /// Set the maximum number of rows returned in one scan batch.
    ///
    /// When [`Self::batch_size_bytes`] is also configured, both limits apply;
    /// whichever is reached first determines the batch size.
    pub fn batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.inner.batch_size(batch_size);
        self
    }

    /// Set the approximate in-memory byte target for one scan batch.
    ///
    /// This composes with [`Self::batch_size`], but cannot be combined with
    /// [`Self::strict_batch_size`] because merging batches can exceed the target.
    pub fn batch_size_bytes(&mut self, batch_size_bytes: u64) -> &mut Self {
        self.inner.batch_size_bytes(batch_size_bytes);
        self
    }

    /// Require full output batches to contain exactly the requested row count.
    ///
    /// The final batch may contain fewer rows. Enabling this with a byte target
    /// (including one inherited from the dataset) fails when the scan executes.
    pub fn strict_batch_size(&mut self, strict_batch_size: bool) -> &mut Self {
        self.inner.strict_batch_size(strict_batch_size);
        self
    }

    /// Apply a row limit and offset.
    pub fn limit(&mut self, limit: Option<i64>, offset: Option<i64>) -> Result<&mut Self> {
        self.inner.limit(limit, offset)?;
        Ok(self)
    }

    /// Include Lance's stable row-id column in the output.
    pub fn with_row_id(&mut self) -> &mut Self {
        self.inner.with_row_id();
        self
    }

    /// Choose how blob columns are represented in scan output.
    pub fn blob_handling(&mut self, blob_handling: BlobHandling) -> &mut Self {
        self.inner.blob_handling(blob_handling);
        self
    }

    /// Execute the configured read without exposing its physical plan.
    pub async fn try_into_stream(&self) -> Result<DatasetRecordBatchStream> {
        self.inner.try_into_stream().await
    }
}

impl SnapshotDataset {
    pub(crate) fn wrap(inner: omnigraph_catalog::SnapshotDataset) -> Self {
        Self { inner }
    }

    /// Build a read-only scanner over this pinned dataset version.
    pub fn scan(&self) -> SnapshotScanner {
        SnapshotScanner::wrap(self.inner.scan())
    }

    /// Count physical rows in this pinned dataset version, optionally with a filter.
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        self.inner.count_rows(filter).await
    }

    /// Lance schema of this pinned dataset version.
    pub fn schema(&self) -> &LanceSchema {
        self.inner.schema()
    }

    /// Lance version of this pinned dataset.
    pub fn published_dataset_version(&self) -> u64 {
        self.inner.published_dataset_version()
    }

    /// Read-only physical index metadata for this pinned dataset version.
    pub async fn load_indices(&self) -> Result<Arc<Vec<IndexMetadata>>> {
        self.inner.load_indices().await
    }

    /// Whether this pinned Lance manifest carries any raw index-metadata
    /// section, without filtering entries by the current reader's supported
    /// index versions.
    ///
    /// An absent section proves an empty physical index inventory. Callers
    /// that require that proof must not substitute [`Self::load_indices`],
    /// whose compatibility filtering can hide unsupported metadata.
    pub fn has_raw_index_section(&self) -> bool {
        self.inner.has_raw_index_section()
    }

    /// Whether this pinned table version depends on files outside its dataset
    /// root through Lance's `base_paths` relocation mechanism.
    ///
    /// The actual paths remain private so this read-only metadata surface
    /// cannot disclose source locations or expose a writable Lance handle.
    pub fn has_external_base_paths(&self) -> bool {
        self.inner.has_external_base_paths()
    }

    /// Whether `column` has complete usable BTREE coverage.
    pub async fn index_coverage(&self, column: &str) -> Result<crate::IndexCoverage> {
        self.inner.index_coverage(column).await
    }

    /// Whether any user index leaves current fragments uncovered.
    pub async fn has_unindexed_fragments(&self) -> Result<bool> {
        self.inner.has_unindexed_fragments().await
    }

    /// Whether this dataset has a user BTREE index on physical `column`.
    pub async fn has_btree_index(&self, column: &str) -> Result<bool> {
        self.inner.has_btree_index(column).await
    }

    /// Whether this dataset has a user full-text index on physical `column`.
    pub async fn has_fts_index(&self, column: &str) -> Result<bool> {
        self.inner.has_fts_index(column).await
    }

    /// Whether this dataset has a user vector index on physical `column`.
    pub async fn has_vector_index(&self, column: &str) -> Result<bool> {
        self.inner.has_vector_index(column).await
    }
}

impl Snapshot {
    pub(crate) fn same_manifest_image(&self, other: &Self) -> bool {
        self.inner.same_manifest_image(&other.inner)
    }

    pub(crate) fn wrap(inner: omnigraph_catalog::Snapshot) -> Self {
        Self { inner }
    }

    #[cfg(any(all(test, feature = "failpoints"), feature = "test-util"))]
    pub(crate) fn raw(&self) -> &omnigraph_catalog::Snapshot {
        &self.inner
    }

    #[cfg(test)]
    pub(crate) fn raw_mut(&mut self) -> &mut omnigraph_catalog::Snapshot {
        &mut self.inner
    }

    /// Load the contract at this snapshot's captured native branch and version.
    pub(crate) async fn read_schema_contract(
        &self,
        root_uri: &str,
    ) -> Result<omnigraph_catalog::SchemaContractRow> {
        omnigraph_catalog::ManifestCoordinator::read_schema_contract_for_snapshot(
            root_uri,
            &self.inner,
        )
        .await
    }

    /// The internal-schema (storage-format) stamp of this snapshot's own
    /// `__manifest` version.
    pub(crate) async fn internal_schema_stamp(&self, root_uri: &str) -> Result<Option<u32>> {
        omnigraph_catalog::ManifestCoordinator::internal_schema_stamp_for_snapshot(
            root_uri,
            &self.inner,
        )
        .await
    }

    pub(crate) fn graph_branch(&self) -> Option<&str> {
        self.inner.graph_branch()
    }

    pub(crate) fn native_branch(&self) -> Option<&str> {
        self.inner.native_branch()
    }

    /// Exact `graph_head:<branch>` commit id from this snapshot's own pinned
    /// graph-manifest version (`None` = main). Absent on a branch with no commits.
    ///
    /// This exact row is write authority when present. A fresh named branch has
    /// no materialized row yet; callers that need its effective inherited
    /// lineage head must resolve it through `GraphCoordinator`.
    pub fn graph_head(&self, branch: Option<&str>) -> Option<&str> {
        self.inner.graph_head(branch)
    }

    /// The identity of the `schema_contract` row in this snapshot's own pinned
    /// graph-manifest version; `None` only for a version written before the
    /// row existed.
    pub(crate) fn schema_contract(&self) -> Option<SchemaContractIdentity> {
        self.inner
            .schema_contract()
            .map(SchemaContractIdentity::from)
    }

    pub(crate) fn bind_catalog_aliases(&mut self, catalog: &Catalog) -> Result<()> {
        self.inner.bind_catalog_aliases(catalog)
    }

    /// Open a backing dataset at its pinned version by qualified graph type key. With read caches present
    /// (live Branch reads), reuse a held handle through the cache (0 open IO on a
    /// warm repeat) and the shared `Session`; otherwise plain-open (Fix 2).
    pub async fn open_dataset(&self, type_key: &str) -> Result<SnapshotDataset> {
        self.inner
            .open_dataset(type_key)
            .await
            .map(SnapshotDataset::wrap)
    }

    pub(crate) async fn open_lance_dataset(&self, type_key: &str) -> Result<Dataset> {
        self.inner.open_lance_dataset(type_key).await
    }

    pub(crate) fn set_read_caches(&mut self, caches: omnigraph_catalog::SnapshotReadCaches) {
        self.inner.set_read_caches(caches);
    }

    /// Graph-manifest version this snapshot was taken from.
    pub fn graph_manifest_version(&self) -> u64 {
        self.inner.graph_manifest_version()
    }

    pub(crate) fn root_uri(&self) -> &str {
        self.inner.root_uri()
    }

    /// Look up backing-dataset metadata by qualified graph type key.
    pub fn dataset(&self, type_key: &str) -> Option<&DatasetEntry> {
        self.inner.dataset(type_key)
    }

    /// Iterate over metadata for every backing dataset in this snapshot.
    pub fn datasets(&self) -> impl Iterator<Item = &DatasetEntry> {
        self.inner.datasets()
    }
}
