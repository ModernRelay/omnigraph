// Lance 6's trait surface (heavier futures/streams nesting around the
// staged-write API in `storage_layer.rs`) pushes us past the default
// trait-resolution recursion limit of 128 on Linux builds. Raising to
// 256 here is the upstream-suggested fix from rustc itself
// ("consider increasing the recursion limit"). macOS happens to short-
// circuit before tripping the limit; CI on Linux does not. Revisit if
// future Lance bumps stop needing this.
#![recursion_limit = "256"]
//! The `__manifest` catalog, an internal crate of the omnigraph engine with no
//! compatibility promise: depend on `omnigraph`, whose facade fences the writers.

pub(crate) use omnigraph_core::dst_ids;
pub(crate) use omnigraph_core::{
    branch_control, branch_names, dataset_index, error, handle_cache, instrumentation,
    lance_access, lance_clone, metadata, seams, staging, storage,
};
pub mod commit_graph;

mod commit;
mod record;
mod row;
mod schema_publication;
pub use schema_publication::{
    SchemaPublicationCandidate, SchemaPublicationEvidence, read_schema_publication_at,
    read_schema_publication_candidate_at,
};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::branch_control::list_live_manifest_branch_contents;
use crate::commit_graph::{CommitGraph, HistoryCache};
use crate::error::{OmniError, Result, missing_graph_type_at_snapshot};
use crate::handle_cache::TableHandleCache;
use crate::seams::{decide_seam, fail};
use datafusion::logical_expr::Expr;
use lance::Dataset;
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance::datatypes::{BlobHandling, Schema as LanceSchema};
use lance::index::DatasetIndexExt;
use lance_namespace::models::CreateTableVersionRequest;
use lance_table::format::IndexMetadata;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_compiler::{SYSTEM_COLUMNS_LEGACY, SYSTEM_COLUMNS_V3, SystemColumns};
use omnigraph_core::graph_commit_id::commit_id_answers;

pub mod graph;
pub mod history;
pub mod layout;
pub mod migrations;
// Test-only since RFC-013 step 3a (compiled under `test` or the `test-util`
// feature the engine's tests enable): with both reads (Fix 2) and writes
// bypassing the Lance namespace, nothing in production routes through it; the
// `LanceNamespace` impls are retained only to validate the contract in tests.
#[cfg(any(test, feature = "test-util"))]
pub mod namespace;
pub mod publisher;
#[cfg(feature = "test-util")]
pub mod read_executor;
pub mod retention;
pub mod state;
pub use branch_names::is_merge_input_tag;

pub use graph::{GenesisManifestAttempt, ManifestInitError};
use graph::{
    OpenedManifest, init_manifest_graph, load_initial_manifest_state, open_exact_genesis_manifest,
    open_manifest_graph, snapshot_state_at,
};
pub use history::HistoryRecord;
#[cfg(test)]
use layout::open_manifest_dataset;
use layout::{
    branch_ref_error, open_manifest_branch_with_identifier,
    open_manifest_dataset_native_with_session, open_manifest_dataset_with_session,
    resolve_native_manifest_branch, table_uri_for_path,
};
pub use layout::{history_uri, manifest_uri};
pub use metadata::TableVersionMetadata;
#[cfg(test)]
use metadata::{
    OMNIGRAPH_ROW_COUNT_KEY, object_store_path_from_uri, table_version_metadata_for_state,
};
pub use migrations::stamp_for_system_columns;
#[cfg(test)]
use namespace::{branch_manifest_namespace, staged_table_namespace};
pub use publisher::{GraphHeadExpectation, LineageIntent, PublishPrecondition};
use publisher::{GraphNamespacePublisher, ManifestBatchPublisher, PublishOutcome};
pub use state::DatasetEntry;
use state::read_schema_contract_row;
#[cfg(test)]
use state::string_column;
pub use state::{BranchRecords, CommitBuffer, ReplacedRow, ReplacedTable};
pub use state::{
    GraphLineageRow, SchemaContractHead, SchemaContractRow, TablePin, TableRow, TableState,
};
use state::{ManifestRows, ManifestState, read_manifest_state};

/// The maximum supported storage-format stamp, the one this binary writes; the
/// served range starts at [`MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION`].
/// Read a graph's per-branch stamp with [`internal_schema_stamp_at`].
pub const INTERNAL_MANIFEST_SCHEMA_VERSION: u32 = migrations::INTERNAL_MANIFEST_SCHEMA_VERSION;
/// The lowest storage-format stamp normal open serves.
pub const MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION: u32 =
    migrations::MIN_SUPPORTED_INTERNAL_SCHEMA_VERSION;
pub use migrations::is_served_stamp;

/// One row per table identity: its registration and its current pin or drop.
pub const OBJECT_TYPE_TABLE: &str = "table";
/// The record of the head graph commit, one row per `__manifest` version,
/// written by the publish CAS that moves the head. `object_id` is the commit's
/// id.
pub const OBJECT_TYPE_SCHEMA_CONTRACT: &str = "schema_contract";
pub const SCHEMA_CONTRACT_OBJECT_ID: &str = "schema_contract";
pub const OBJECT_TYPE_GRAPH_COMMIT: &str = "graph_commit";
/// The record of a buffered commit: a first-parent ancestor of the head that
/// the branch has not appended to `__history`. `object_id` is the commit's id.
pub const OBJECT_TYPE_SETTLED_COMMIT: &str = "settled_commit";
/// What a publish read for a table identity it then changed, kept while a
/// buffered commit is older than that publish.
pub const OBJECT_TYPE_REPLACED_TABLE: &str = "replaced_table";
/// The commit ceiling of the buffer of a `__manifest` version. The buffer is
/// released on [`HISTORY_RELEASE_BYTES`]; the ceiling only bounds a buffer of
/// tiny commits. Equal to the slot ceiling of a history block.
pub const TAIL_MAX_COMMITS: usize = 16 * 1024;
/// The byte budget of the buffer, in the measure of
/// [`CommitBuffer::buffered_bytes`]: the commit that reaches it closes its
/// history block, and the next publish writes the buffered commits to `__history`.
/// The production budget and the ceiling; a session's `history_release_bytes`
/// setting lowers it for that session's publishes, carried on
/// [`LineageIntent::history_release_bytes`].
pub const HISTORY_RELEASE_BYTES: usize = 256 * 1024;

/// The byte budget one publish measures its buffer against: the production
/// [`HISTORY_RELEASE_BYTES`] or a session's lower `history_release_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryReleaseBytes(pub usize);

impl HistoryReleaseBytes {
    /// The production budget, [`HISTORY_RELEASE_BYTES`].
    pub const PRODUCTION: Self = Self(HISTORY_RELEASE_BYTES);
}

impl Default for HistoryReleaseBytes {
    fn default() -> Self {
        Self(HISTORY_RELEASE_BYTES)
    }
}

/// The key of the main branch in [`ManifestState::graph_heads`], matching the
/// `"main"` sentinel already used by `SnapshotId::synthetic` and
/// `open_for_branch`.
pub const MAIN_BRANCH_HEAD_KEY: &str = "main";

/// The result of a manifest commit that may have folded in a graph commit
/// (RFC-013 Phase 7).
#[derive(Debug, Clone)]
pub struct CommitOutcome {
    /// The new `__manifest` version after the publish.
    pub version: u64,
    /// The parent the publisher resolved for the recorded commit, or `None` when
    /// no lineage was recorded. Lets the caller update its in-memory commit
    /// cache without re-reading the manifest.
    pub parent_commit_id: Option<String>,
    /// The record of the commit the publish wrote, `None` when no lineage was
    /// recorded.
    pub commit: Option<GraphLineageRow>,
}

/// The on-disk internal-schema stamp of `__manifest` at `branch` (main when
/// `None`), or `None` when no parseable stamp exists (a torn init by an
/// older binary, a genuine pre-stamp store, or corrupt metadata — see
/// `migrations::guard_stamp`, which the open paths use instead). Surfaces
/// the storage version to operators (`omnigraph snapshot`).
pub async fn internal_schema_stamp_at(root_uri: &str, branch: Option<&str>) -> Result<Option<u32>> {
    let control_session = crate::lance_access::control_session();
    let dataset = open_manifest_dataset_with_session(root_uri, branch, &control_session).await?;
    Ok(migrations::read_stamp(&dataset))
}

/// Read main's supported storage-format stamp before either graph-open path
/// loads data. The publisher separately validates the selected branch before
/// publication; supported bounds are owned by [`migrations::guard_stamp`].
pub async fn read_supported_internal_schema_version(root_uri: &str) -> Result<u32> {
    let control_session = crate::lance_access::control_session();
    let dataset = open_manifest_dataset_with_session(root_uri, None, &control_session).await?;
    migrations::guard_stamp(&dataset)
}

/// Whether the selected graph-manifest dataset depends on files outside its
/// own dataset root.
///
/// A directory copy is self-contained only when both this manifest dataset
/// and every user dataset have no Lance `base_paths`. Keep the paths private:
/// callers need a relocation eligibility bit, not access to storage
/// locations that may contain credentials or deployment details.
pub async fn manifest_has_external_base_paths(
    root_uri: &str,
    branch: Option<&str>,
) -> Result<bool> {
    let control_session = crate::lance_access::control_session();
    let dataset = open_manifest_dataset_with_session(root_uri, branch, &control_session).await?;
    Ok(!dataset.manifest().base_paths.is_empty())
}

/// Where the text of a snapshot's schema contract is read.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ContractSource {
    /// The `__manifest` version the snapshot was captured from.
    Manifest,
    /// The content a commit record names under `__history/schemas/`.
    Archive(Option<String>),
}

/// Immutable point-in-time view of the database.
///
/// Cheap to create (no storage I/O). All reads within a query go through one
/// Snapshot to guarantee cross-type consistency.
#[derive(Debug, Clone)]
pub struct Snapshot {
    root_uri: String,
    pub version: u64,
    pub entries: HashMap<String, DatasetEntry>,
    /// Exact materialized `graph_head:<branch>` commit ids from this SAME
    /// pinned manifest version (see `ManifestState::graph_heads`). Carried so
    /// a read can report the commit id of the world it was served from —
    /// resolving the head separately (e.g. via `CommitGraph`) could pair this
    /// snapshot's datasets with a different version's head.
    pub graph_heads: HashMap<String, String>,
    /// The `schema_contract` row's head from this SAME pinned manifest
    /// version (see `ManifestState::schema_contract`).
    schema_contract: Option<SchemaContractHead>,
    contract_source: ContractSource,
    manifest_dataset: Option<Dataset>,
    captured_contract: Option<Arc<SchemaContractRow>>,
    /// Logical graph branch used to capture this snapshot, including historical reads.
    graph_branch: Option<String>,
    /// Native ref of the live branch coordinator; named writes record it as fork owner.
    /// `None` on main and directly built test snapshots.
    native_branch: Option<String>,
    /// Per-graph read caches (shared `Session` + held-handle cache), injected by
    /// `Omnigraph::resolved_target` for live Branch reads so dataset opens reuse
    /// handles (0 IO on a warm repeat) and one `Session`. `None` for write-prelude
    /// snapshots, time-travel / Snapshot-id reads, and directly-built test
    /// snapshots, which fall back to a plain open.
    read_caches: Option<SnapshotReadCaches>,
}

#[derive(Clone)]
pub struct SnapshotReadCaches {
    pub session: Arc<lance::session::Session>,
    pub handles: Arc<TableHandleCache>,
}

impl std::fmt::Debug for SnapshotReadCaches {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SnapshotReadCaches").finish_non_exhaustive()
    }
}

pub fn is_edge_table_key(table_key: &str) -> bool {
    table_key.starts_with("edge:")
}

/// The vintage a system identity column's spelling belongs to.
fn vintage_of_identity_spelling(id: &str, table_key: &str) -> Result<SystemColumns> {
    if id == SYSTEM_COLUMNS_LEGACY.id {
        Ok(SYSTEM_COLUMNS_LEGACY)
    } else if id == SYSTEM_COLUMNS_V3.id {
        Ok(SYSTEM_COLUMNS_V3)
    } else {
        Err(OmniError::manifest_internal(format!(
            "table '{table_key}': its system identity column is spelled '{id}', which is \
             neither 'id' nor '__id'"
        )))
    }
}

/// Resolve system column spellings from a pinned image's exact non-null Utf8 primary key.
pub fn system_columns_at_image(image: &LanceSchema, table_key: &str) -> Result<SystemColumns> {
    let primary_key = image.unenforced_primary_key();
    let [id] = primary_key.as_slice() else {
        return Err(OmniError::manifest_internal(format!(
            "table '{table_key}': the pinned image has {} unenforced primary key fields, \
             expected exactly the system identity column",
            primary_key.len()
        )));
    };
    if id.nullable || id.data_type() != arrow_schema::DataType::Utf8 {
        return Err(OmniError::manifest_internal(format!(
            "table '{table_key}': the system identity primary key must be non-null Utf8"
        )));
    }
    let vintage = vintage_of_identity_spelling(&id.name, table_key)?;
    if is_edge_table_key(table_key)
        && (image.field(vintage.src).is_none() || image.field(vintage.dst).is_none())
    {
        return Err(OmniError::manifest_internal(format!(
            "table '{table_key}': the pinned image lacks the '{}'/'{}' endpoints of its \
             identity column's vintage",
            vintage.src, vintage.dst
        )));
    }
    Ok(vintage)
}

/// Read-only view of one backing dataset pinned by a [`Snapshot`].
///
/// The underlying Lance [`Dataset`] is deliberately private: a snapshot dataset
/// can scan rows and inspect read metadata, but it cannot reach Lance's
/// mutating APIs or advance a dataset HEAD outside OmniGraph's coordinated write
/// path.
#[derive(Debug, Clone)]
pub struct SnapshotDataset {
    dataset: Dataset,
}

/// Read-only scan builder for a [`SnapshotDataset`].
///
/// This forwards scan configuration and execution, but not Lance's raw
/// [`Scanner`] or physical-plan construction. A Lance physical scan plan
/// exposes its embedded [`Dataset`], which would let SDK callers recover a
/// writable handle and bypass graph publication.
pub struct SnapshotScanner {
    scanner: Scanner,
    dataset: Dataset,
}

impl SnapshotScanner {
    /// Select the output columns.
    pub fn project<T: AsRef<str>>(&mut self, columns: &[T]) -> Result<&mut Self> {
        self.scanner.project(columns).map_err(OmniError::storage)?;
        Ok(self)
    }

    /// Apply a SQL filter expression.
    pub fn filter(&mut self, filter: &str) -> Result<&mut Self> {
        self.scanner.filter(filter).map_err(OmniError::storage)?;
        Ok(self)
    }

    /// Apply a structured DataFusion filter expression.
    pub fn filter_expr(&mut self, filter: Expr) -> &mut Self {
        self.scanner.filter_expr(filter);
        self
    }

    /// Set the maximum number of rows returned in one scan batch.
    ///
    /// When [`Self::batch_size_bytes`] is also configured, both limits apply;
    /// whichever is reached first determines the batch size.
    pub fn batch_size(&mut self, batch_size: usize) -> &mut Self {
        self.scanner.batch_size(batch_size);
        self
    }

    /// Set the approximate in-memory byte target for one scan batch.
    ///
    /// This composes with [`Self::batch_size`], but cannot be combined with
    /// [`Self::strict_batch_size`] because merging batches can exceed the target.
    pub fn batch_size_bytes(&mut self, batch_size_bytes: u64) -> &mut Self {
        self.scanner.batch_size_bytes(batch_size_bytes);
        self
    }

    /// Require full output batches to contain exactly the requested row count.
    ///
    /// The final batch may contain fewer rows. Enabling this with a byte target
    /// (including one inherited from the dataset) fails when the scan executes.
    pub fn strict_batch_size(&mut self, strict_batch_size: bool) -> &mut Self {
        self.scanner.strict_batch_size(strict_batch_size);
        self
    }

    /// Apply a row limit and offset.
    pub fn limit(&mut self, limit: Option<i64>, offset: Option<i64>) -> Result<&mut Self> {
        self.scanner
            .limit(limit, offset)
            .map_err(OmniError::storage)?;
        Ok(self)
    }

    /// Include Lance's stable row-id column in the output.
    pub fn with_row_id(&mut self) -> &mut Self {
        self.scanner.with_row_id();
        self
    }

    /// Choose how blob columns are represented in scan output.
    pub fn blob_handling(&mut self, blob_handling: BlobHandling) -> &mut Self {
        self.scanner.blob_handling(blob_handling);
        self
    }

    /// Execute the configured read without exposing its physical plan.
    pub async fn try_into_stream(&self) -> Result<DatasetRecordBatchStream> {
        crate::dataset_index::validate_full_text_scan(&self.dataset, &self.scanner, None).await?;
        self.scanner
            .try_into_stream()
            .await
            .map_err(OmniError::storage)
    }
}

impl SnapshotDataset {
    pub fn new(dataset: Dataset) -> Self {
        Self { dataset }
    }

    /// Build a read-only scanner over this pinned dataset version.
    pub fn scan(&self) -> SnapshotScanner {
        SnapshotScanner {
            scanner: self.dataset.scan(),
            dataset: self.dataset.clone(),
        }
    }

    /// Count physical rows in this pinned dataset version, optionally with a filter.
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        if let Some(filter) = &filter {
            let mut scanner = self.dataset.scan();
            scanner.filter(filter).map_err(OmniError::storage)?;
            crate::dataset_index::validate_full_text_scan(&self.dataset, &scanner, None).await?;
        }
        self.dataset
            .count_rows(filter)
            .await
            .map_err(OmniError::storage)
    }

    /// Lance schema of this pinned dataset version.
    pub fn schema(&self) -> &LanceSchema {
        self.dataset.schema()
    }

    /// Lance version of this pinned dataset.
    pub fn published_dataset_version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Read-only physical index metadata for this pinned dataset version.
    pub async fn load_indices(&self) -> Result<Arc<Vec<IndexMetadata>>> {
        self.dataset
            .load_indices()
            .await
            .map_err(OmniError::storage)
    }

    /// Whether this pinned Lance manifest carries any raw index-metadata
    /// section, without filtering entries by the current reader's supported
    /// index versions.
    ///
    /// An absent section proves an empty physical index inventory. Callers
    /// that require that proof must not substitute [`Self::load_indices`],
    /// whose compatibility filtering can hide unsupported metadata.
    pub fn has_raw_index_section(&self) -> bool {
        self.dataset.manifest().index_section.is_some()
    }

    /// Whether this pinned table version depends on files outside its dataset
    /// root through Lance's `base_paths` relocation mechanism.
    ///
    /// The actual paths remain private so this read-only metadata surface
    /// cannot disclose source locations or expose a writable Lance handle.
    pub fn has_external_base_paths(&self) -> bool {
        !self.dataset.manifest().base_paths.is_empty()
    }

    /// Whether `column` has complete usable BTREE coverage.
    pub async fn index_coverage(
        &self,
        column: &str,
    ) -> Result<crate::dataset_index::IndexCoverage> {
        crate::dataset_index::key_column_index_coverage(&self.dataset, column).await
    }

    /// Whether any user index leaves current fragments uncovered.
    pub async fn has_unindexed_fragments(&self) -> Result<bool> {
        crate::dataset_index::has_unindexed_fragments(&self.dataset).await
    }

    /// Whether this dataset has a user BTREE index on physical `column`.
    pub async fn has_btree_index(&self, column: &str) -> Result<bool> {
        crate::dataset_index::has_btree_index_on(&self.dataset, column).await
    }

    /// Whether this dataset has a user full-text index on physical `column`.
    pub async fn has_fts_index(&self, column: &str) -> Result<bool> {
        crate::dataset_index::has_fts_index_on(&self.dataset, column).await
    }

    /// Whether this dataset has a user vector index on physical `column`.
    pub async fn has_vector_index(&self, column: &str) -> Result<bool> {
        crate::dataset_index::has_vector_index_on(&self.dataset, column).await
    }
}

impl Snapshot {
    pub fn graph_branch(&self) -> Option<&str> {
        self.graph_branch.as_deref()
    }

    /// The native Lance ref this branch snapshot was served from (`None` on
    /// main, for time-travel reads, and for directly built snapshots).
    pub fn native_branch(&self) -> Option<&str> {
        self.native_branch.as_deref()
    }

    /// Exact `graph_head:<branch>` commit id from this snapshot's own pinned
    /// graph-manifest version (`None` = main). Absent on a branch with no commits.
    ///
    /// This exact row is write authority when present. A fresh named branch has
    /// no materialized row yet; callers that need its effective inherited
    /// lineage head must resolve it through `GraphCoordinator`.
    pub fn graph_head(&self, branch: Option<&str>) -> Option<&str> {
        let branch_key = branch.unwrap_or(MAIN_BRANCH_HEAD_KEY);
        self.graph_heads.get(branch_key).map(String::as_str)
    }

    /// The schema contract's head (IR hash, identity version and domain) from
    /// this snapshot's own pinned manifest version. `None` only for a version
    /// written before the contract lived in `__manifest` (a time-travel read
    /// of a stamp-12 version) or a directly built test snapshot.
    pub fn schema_contract(&self) -> Option<&SchemaContractHead> {
        self.schema_contract.as_ref()
    }

    /// Whether both snapshots captured the same immutable manifest image.
    pub fn same_manifest_image(&self, other: &Self) -> bool {
        self.root_uri == other.root_uri
            && self.version == other.version
            && self.native_branch == other.native_branch
            && self.schema_contract == other.schema_contract
            && match (&self.manifest_dataset, &other.manifest_dataset) {
                (Some(left), Some(right)) => publisher::manifest_image_matches(
                    left.manifest(),
                    left.manifest_location(),
                    right,
                ),
                _ => false,
            }
    }

    /// Bind the current accepted catalog's aliases onto a historical snapshot
    /// by immutable table identity for query execution.
    ///
    /// Public historical snapshots retain their original aliases. This method
    /// is used only on the operation-local copy behind `run_query_at` and an
    /// explicit snapshot-target query, whose source is typechecked against the
    /// current catalog. A pure type rename can therefore address the same old
    /// table lifetime under its current name. Conversely, a reused name whose
    /// current identity is absent from the historical snapshot is removed from
    /// the execution view, preventing cross-incarnation adoption.
    pub fn bind_catalog_aliases(&mut self, catalog: &Catalog) -> Result<()> {
        // Freeze the historical identity map before changing any aliases. A
        // renamed-away alias may later be reused by a new live type; resolving
        // that replacement first must not remove the only entry needed to bind
        // the original identity under its current renamed alias.
        let historical_by_identity = self
            .entries
            .values()
            .cloned()
            .map(|entry| (entry.identity, entry))
            .collect::<HashMap<_, _>>();
        let schema_ir = catalog.bound_schema_ir().ok_or_else(|| {
            OmniError::manifest_internal(
                "historical query alias binding requires an identity-bound catalog".to_string(),
            )
        })?;
        let table_aliases = schema_ir
            .nodes
            .iter()
            .map(|node| {
                (
                    format!("node:{}", node.name),
                    node.type_id.get(),
                    node.table_incarnation_id.get(),
                )
            })
            .chain(schema_ir.edges.iter().map(|edge| {
                (
                    format!("edge:{}", edge.name),
                    edge.type_id.get(),
                    edge.table_incarnation_id.get(),
                )
            }))
            .collect::<Vec<_>>();

        for (table_key, stable_table_id, incarnation_id) in table_aliases {
            let identity = TableIdentity::new(stable_table_id, incarnation_id)?;

            // An exact alias belonging to another lifetime is actively unsafe:
            // remove it before looking for this catalog type's identity under a
            // historical name.
            self.entries.remove(&table_key);
            if let Some(entry) = historical_by_identity.get(&identity) {
                self.entries.insert(table_key, entry.clone());
            }
        }
        Ok(())
    }

    /// Open a backing dataset at its pinned version by qualified graph type key. With read caches present
    /// (live Branch reads), reuse a held handle through the cache (0 open IO on a
    /// warm repeat) and the shared `Session`; otherwise plain-open (Fix 2).
    pub async fn open_dataset(&self, type_key: &str) -> Result<SnapshotDataset> {
        self.open_lance_dataset(type_key)
            .await
            .map(SnapshotDataset::new)
    }

    /// Open the raw Lance dataset for engine-internal read execution.
    ///
    /// `pub` only because the engine crate calls it across the crate boundary;
    /// it is hidden from the documented surface. The engine wraps this type in
    /// its facade `Snapshot` (`crates/omnigraph/src/db/snapshot.rs`) as a
    /// private field, so no engine consumer can reach this method; a
    /// direct dependency on `omnigraph-catalog` steps outside the engine's
    /// contract.
    #[doc(hidden)]
    pub async fn open_lance_dataset(&self, type_key: &str) -> Result<Dataset> {
        let entry = self
            .entries
            .get(type_key)
            .ok_or_else(|| OmniError::manifest(missing_graph_type_at_snapshot(type_key)))?;
        match &self.read_caches {
            Some(caches) => {
                let location = table_uri_for_path(
                    &self.root_uri,
                    &entry.dataset_path,
                    entry.native_dataset_branch.as_deref(),
                )?;
                caches
                    .handles
                    .get_or_open(
                        &entry.dataset_path,
                        entry.native_dataset_branch.as_deref(),
                        entry.published_dataset_version,
                        entry.version_metadata.e_tag(),
                        entry.version_metadata.staged_version(),
                        entry.version_metadata.transaction_uuid(),
                        entry.version_metadata.last_linear_version(),
                        &location,
                        Some(&caches.session),
                    )
                    .await
            }
            None => open_dataset_entry(entry, &self.root_uri, None).await,
        }
    }

    /// Attach per-graph read caches (shared `Session` + handle cache) so this
    /// snapshot's dataset opens reuse handles and the session. Set by
    /// `Omnigraph::resolved_target` for live Branch reads only.
    pub fn set_read_caches(&mut self, caches: SnapshotReadCaches) {
        self.read_caches = Some(caches);
    }

    /// Graph-manifest version this snapshot was taken from.
    pub fn graph_manifest_version(&self) -> u64 {
        self.version
    }

    /// Store root this snapshot's datasets live under (the object-store
    /// prefix). Consumed by the graph-index artifact path derivation.
    pub fn root_uri(&self) -> &str {
        &self.root_uri
    }

    /// Look up backing-dataset metadata by qualified graph type key.
    pub fn dataset(&self, type_key: &str) -> Option<&DatasetEntry> {
        self.entries.get(type_key)
    }

    /// Iterate over metadata for every backing dataset in this snapshot.
    pub fn datasets(&self) -> impl Iterator<Item = &DatasetEntry> {
        self.entries.values()
    }
}

impl HistoryRecord {
    /// The snapshot of the graph as of this commit, built from the `table`
    /// rows the record carries. Its schema contract is read from the content
    /// the commit names under `__history/schemas/`, never from a `__manifest`
    /// version.
    pub fn snapshot(&self, root_uri: &str) -> Result<Snapshot> {
        let state = state::manifest_state(
            &self.tables,
            &self.commit,
            self.commit.graph_manifest_version,
            self.commit.native_branch.as_deref(),
        )?;
        let mut snapshot = ManifestCoordinator::snapshot_from_state(root_uri, state);
        snapshot.contract_source = ContractSource::Archive(self.commit.schema_content_hash.clone());
        snapshot.graph_branch = self.commit.graph_branch.clone();
        snapshot.native_branch = self.commit.native_branch.clone();
        Ok(snapshot)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestIncarnation {
    pub version: u64,
    pub e_tag: Option<String>,
    timestamp_nanos: Option<u128>,
    branch_identifier: lance::dataset::refs::BranchIdentifier,
}

impl ManifestIncarnation {
    pub fn matches(&self, held: &Self) -> bool {
        if self.version != held.version || self.branch_identifier != held.branch_identifier {
            return false;
        }
        match (&self.e_tag, &held.e_tag) {
            (Some(latest), Some(current)) => latest == current,
            _ => match (self.timestamp_nanos, held.timestamp_nanos) {
                (Some(latest), Some(current)) => latest == current,
                // Some object stores can omit both e_tag and manifest timestamp
                // from the reachable API. In that narrow case the version-number
                // probe is the strongest available identity.
                _ => true,
            },
        }
    }
}

/// Immutable probe handle captured from one exact manifest dataset
/// incarnation.
///
/// It retains only Lance's pinned Dataset handle plus the active branch and
/// incarnation token. It does not carry mutable graph-head state or a lineage
/// projection, so callers cannot mistake it for publish authority.
#[derive(Debug, Clone)]
pub struct CapturedManifestProbe {
    dataset: Dataset,
    active_branch: Option<String>,
    captured: ManifestIncarnation,
}

impl CapturedManifestProbe {
    pub async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        crate::instrumentation::record_probe();
        probe_dataset_latest_incarnation(&self.dataset, self.active_branch.as_deref()).await
    }

    pub async fn is_current(&self) -> Result<bool> {
        Ok(self
            .probe_latest_incarnation()
            .await?
            .matches(&self.captured))
    }
}

async fn probe_dataset_latest_incarnation(
    dataset: &Dataset,
    active_branch: Option<&str>,
) -> Result<ManifestIncarnation> {
    if active_branch.is_none() {
        return Ok(ManifestIncarnation {
            version: dataset
                .latest_version_id()
                .await
                .map_err(OmniError::storage)?,
            e_tag: dataset.manifest_location().e_tag.clone(),
            timestamp_nanos: Some(dataset.manifest().timestamp_nanos),
            branch_identifier: lance::dataset::refs::BranchIdentifier::main(),
        });
    }
    let branch = active_branch.expect("named-branch arm checked above");
    // A named branch's native identifier is its lifetime witness. Pair it with
    // the branch-local latest version instead of loading the manifest body:
    // version changes catch ordinary commits, while the identifier catches a
    // same-source delete/recreate even when version, e-tag, and timestamp all
    // repeat. Read the version first so a recreation between the two probes
    // yields the replacement identifier rather than a false match to the held
    // lifetime.
    let held = async {
        let version = dataset
            .latest_version_id()
            .await
            .map_err(|error| branch_ref_error(error, branch))?;
        let native = dataset.manifest().branch.as_deref().ok_or_else(|| {
            OmniError::manifest_internal("named coordinator has no native manifest ref")
        })?;
        let branch_identifier =
            crate::branch_control::get_live_manifest_branch_contents(dataset, native)
                .await?
                .identifier;
        Ok::<_, OmniError>(ManifestIncarnation {
            version,
            e_tag: None,
            timestamp_nanos: None,
            branch_identifier,
        })
    }
    .await;
    match held {
        Ok(incarnation) => return Ok(incarnation),
        Err(OmniError::BranchNotFound { .. } | OmniError::Storage(_)) => {}
        Err(error) => return Err(error),
    }
    let native = resolve_native_manifest_branch(dataset, branch).await?;
    let replacement = dataset
        .checkout_branch(&native)
        .await
        .map_err(|error| branch_ref_error(error, branch))?;
    let branch_identifier = crate::branch_control::dataset_branch_identifier(&replacement)
        .await
        .map_err(|error| branch_ref_error(error, branch))?;
    Ok(ManifestIncarnation {
        version: replacement.version().version,
        e_tag: None,
        timestamp_nanos: None,
        branch_identifier,
    })
}

impl DatasetUpdate {
    pub fn to_create_table_version_request(&self) -> CreateTableVersionRequest {
        self.version_metadata.to_create_table_version_request(
            &self.type_key,
            self.published_dataset_version,
            self.entity_count,
            self.native_dataset_branch.as_deref(),
        )
    }
}

/// Immutable graph-level identity of one physical table lifetime.
///
/// `stable_table_id` survives supported type renames. Dropping and re-adding a
/// type mints a new `table_incarnation_id`, so old version and tombstone rows
/// can never alias the replacement even when it reuses the same display name.
/// Both components are non-zero; zero remains the sentinel for "no table" in
/// external formats and is never persisted on a table-bearing manifest row.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct TableIdentity {
    pub stable_table_id: u64,
    pub table_incarnation_id: u64,
}

impl TableIdentity {
    pub fn new(stable_table_id: u64, table_incarnation_id: u64) -> Result<Self> {
        let identity = Self {
            stable_table_id,
            table_incarnation_id,
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn validate(self) -> Result<()> {
        if self.stable_table_id == 0 || self.table_incarnation_id == 0 {
            return Err(OmniError::manifest(format!(
                "table identity components must be non-zero (stable_table_id={}, \
                 table_incarnation_id={})",
                self.stable_table_id, self.table_incarnation_id
            )));
        }
        Ok(())
    }
}

impl std::fmt::Display for TableIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:016x}:{:016x}",
            self.stable_table_id, self.table_incarnation_id
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRegistration {
    pub identity: TableIdentity,
    pub table_key: String,
    pub table_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableTombstone {
    pub identity: TableIdentity,
    pub table_key: String,
    pub tombstone_version: u64,
}

/// A live graph branch's `__manifest` as the collector reads it: one open,
/// every observation from that open, so a publication is visible to all of
/// them or to none.
#[derive(Clone)]
pub struct CollectorBranch {
    root_uri: String,
    branch: Option<String>,
    dataset: Dataset,
    identifier: lance::dataset::refs::BranchIdentifier,
}

impl CollectorBranch {
    pub fn head_version(&self) -> u64 {
        self.dataset.version().version
    }

    pub fn identifier(&self) -> &lance::dataset::refs::BranchIdentifier {
        &self.identifier
    }

    /// A fresh authority inventory; the held dataset caches no branch refs.
    pub async fn live_identifiers(&self) -> Result<Vec<lance::dataset::refs::BranchIdentifier>> {
        let mut identifiers =
            crate::branch_control::list_live_manifest_branch_contents(&self.dataset)
                .await?
                .into_values()
                .map(|contents| contents.identifier)
                .collect::<Vec<_>>();
        identifiers.push(lance::dataset::refs::BranchIdentifier::main());
        Ok(identifiers)
    }

    pub fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    /// The commit graph of the head the opened version holds, the same
    /// immutable dataset as the table roots, over the settled commits
    /// `history` has read.
    pub async fn commit_graph(
        &self,
        history: &crate::commit_graph::HistoryCache,
    ) -> Result<crate::commit_graph::CommitGraph> {
        let rows = state::read_manifest_rows_projected(&self.dataset).await?;
        Ok(crate::commit_graph::CommitGraph::from_head(
            &self.root_uri,
            self.dataset.session(),
            rows.head,
            rows.buffer.commits(),
            history.clone(),
        ))
    }

    /// The version history up to the opened version; a version published
    /// after the open is listed by the store and dropped here.
    pub async fn versions(&self) -> Result<Vec<lance::dataset::Version>> {
        let head = self.head_version();
        Ok(self
            .dataset
            .versions()
            .await
            .map_err(OmniError::storage)?
            .into_iter()
            .filter(|version| version.version <= head)
            .collect())
    }

    /// Every pin the branch has held up to the opened version.
    pub async fn rows(&self) -> Result<Vec<DatasetEntry>> {
        state::read_manifest_entries(&self.root_uri, &self.dataset).await
    }

    /// The snapshot at the opened version.
    pub async fn snapshot(&self) -> Result<Snapshot> {
        let mut snapshot = ManifestCoordinator::snapshot_from_state(
            &self.root_uri,
            read_manifest_state(&self.dataset).await?,
        );
        snapshot.graph_branch = self.branch.clone();
        snapshot.native_branch = self.dataset.manifest().branch.clone();
        Ok(snapshot)
    }
}

/// Metadata-only rebinding of one live table identity to a new alias.
///
/// `table_path` is the path the caller observed. The publisher requires it to
/// equal the currently registered path and rewrites only the registration row;
/// no `table_version` row is emitted, so the Lance version is preserved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRename {
    pub identity: TableIdentity,
    pub expected_table_key: String,
    pub table_key: String,
    pub table_path: String,
}

#[derive(Debug, Clone)]
pub enum ManifestChange {
    SchemaContract(SchemaContractRow),
    Update(DatasetUpdate),
    RegisterTable(TableRegistration),
    RenameTable(TableRename),
    Tombstone(TableTombstone),
}

/// One table-version authority assertion supplied to a publish attempt.
///
/// The map key is the immutable table identity. `table_key` is retained only
/// as a diagnostic binding and is itself checked, so a caller prepared before a
/// metadata-only rename cannot accidentally publish against the renamed table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableVersionExpectation {
    pub table_key: String,
    pub table_version: u64,
    pub native_ref: NativeRefPin,
}

/// The native ref a pinned data version was read on. `Exact` pins the
/// registration itself, `Exact(None)` is the root lineage; `Unchecked` is for a
/// producer that pins a data version without a registration in hand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeRefPin {
    Unchecked,
    Exact(Option<String>),
}

pub type ExpectedTableVersions = HashMap<TableIdentity, TableVersionExpectation>;

/// Open this dataset at its pinned version directly by location (Fix 2),
/// without the Lance namespace — which would full-scan `__manifest` twice per
/// open (`describe_table` + `describe_table_version`). The resolved Snapshot
/// already holds the path, version, and branch. Branches are Lance native
/// branches, so `with_branch` resolves `{base}/tree/{branch}` from the base
/// URI; main uses `with_version`.
pub async fn open_dataset_entry(
    entry: &DatasetEntry,
    root_uri: &str,
    session: Option<&Arc<lance::session::Session>>,
) -> Result<Dataset> {
    // The branch-qualified location is the dataset that physically holds this
    // version: main at `{table_path}`, a branch at
    // `{table_path}/tree/{branch}` (Lance native-branch storage). `with_version`
    // then resolves the version within THAT dataset's `_versions` — a branch
    // version lives under `tree/{branch}/_versions`, not the base. This
    // matches the physical layout the namespace path resolved, without the
    // per-open `__manifest` scan.
    let location = table_uri_for_path(
        root_uri,
        &entry.dataset_path,
        entry.native_dataset_branch.as_deref(),
    )?;
    // Route through the one opener (Fix 3). With no session this is exactly
    // the Fix-2 `from_uri(location).with_version`. This is the uncached
    // fallback (a snapshot detached from its graph's read caches); the
    // cached path (`Snapshot::open_lance_dataset` → handle cache) calls the same opener on
    // a miss with the shared session, so both paths count on the per-query
    // `table_wrapper`.
    crate::instrumentation::open_pinned_dataset(
        &location,
        entry.published_dataset_version,
        entry.version_metadata.staged_version(),
        entry.version_metadata.transaction_uuid(),
        entry.version_metadata.last_linear_version(),
        session,
        crate::instrumentation::table_wrapper(),
    )
    .await
}

pub fn table_path_for_identity(table_key: &str, identity: TableIdentity) -> Result<String> {
    if table_key.strip_prefix("node:").is_some() {
        return Ok(format!(
            "nodes/{:016x}-{:016x}",
            identity.stable_table_id, identity.table_incarnation_id
        ));
    }
    if is_edge_table_key(table_key) {
        return Ok(format!(
            "edges/{:016x}-{:016x}",
            identity.stable_table_id, identity.table_incarnation_id
        ));
    }
    Err(OmniError::manifest(format!(
        "invalid table key '{}'",
        table_key
    )))
}

/// An update to apply to the manifest via `commit`.
#[derive(Debug, Clone)]
pub struct DatasetUpdate {
    pub identity: TableIdentity,
    pub type_key: String,
    pub published_dataset_version: u64,
    pub native_dataset_branch: Option<String>,
    pub entity_count: u64,
    pub version_metadata: TableVersionMetadata,
}

/// Coordinates cross-dataset state through the namespace `__manifest` table.
///
/// A `table` row registers a table and carries its current pin; the publish
/// that replaces the rows of a branch's `__manifest` is the graph publish
/// boundary, and the rows of one version are the graph snapshot.
pub struct PreparedManifestOpen {
    root_uri: String,
    dataset: Dataset,
}

pub struct ManifestCoordinator {
    captured_contract: Option<Arc<SchemaContractRow>>,
    root_uri: String,
    dataset: Dataset,
    pub known_state: ManifestState,
    active_branch: Option<String>,
    /// The native Lance ref `active_branch` resolved to at open time
    /// (`{logical}.{incarnation}`, or the bare name for a legacy ref). `None`
    /// on main. Forks copy this exact name, so a recreated branch never
    /// shares physical paths with a dead incarnation.
    native_branch: Option<String>,
    /// Lance-native lifetime captured coherently with `dataset` and
    /// `known_state`. A named ref keeps this value across ordinary commits and
    /// receives a new value after delete/recreate.
    branch_identifier: lance::dataset::refs::BranchIdentifier,
    publisher: Arc<dyn ManifestBatchPublisher>,
    /// The head commit's record and the `table` rows of the `__manifest`
    /// version `known_state` was read at.
    head: Arc<HistoryRecord>,
    /// The buffer of that same version.
    buffer: Arc<CommitBuffer>,
    history: HistoryCache,
}

decide_seam! {
    /// A refresh has opened and decoded a replacement manifest and has not
    /// installed it. Failure here must leave the old coordinator coherent.
    /// Crossed by reads only, which no case step kind names yet.
    pub static READ_REFRESH_POST_STATE_PRE_LINEAGE = ("read.refresh_post_state_pre_lineage", Unreachable, [Fail]);
}

impl ManifestCoordinator {
    /// Take an operation-local copy of this exact immutable view. The caller
    /// still checks its authority after acquiring the operation's gates.
    pub fn capture(&self) -> Self {
        Self {
            captured_contract: self.captured_contract.clone(),
            root_uri: self.root_uri.clone(),
            dataset: self.dataset.clone(),
            known_state: self.known_state.clone(),
            active_branch: self.active_branch.clone(),
            native_branch: self.native_branch.clone(),
            branch_identifier: self.branch_identifier.clone(),
            publisher: Arc::clone(&self.publisher),
            head: Arc::clone(&self.head),
            buffer: Arc::clone(&self.buffer),
            history: self.history.clone(),
        }
    }

    fn default_batch_publisher(
        root_uri: &str,
        active_branch: Option<&str>,
        control_session: Arc<lance::session::Session>,
    ) -> Arc<dyn ManifestBatchPublisher> {
        Arc::new(GraphNamespacePublisher::new_with_session(
            root_uri,
            active_branch,
            control_session,
        ))
    }

    /// A coordinator of `active_branch` over one opened `__manifest` version,
    /// with the default publisher and no settled commit read.
    fn from_opened(root_uri: &str, opened: OpenedManifest, active_branch: Option<String>) -> Self {
        let OpenedManifest {
            dataset,
            known_state,
            rows,
            branch_identifier,
            native_branch,
        } = opened;
        let publisher =
            Self::default_batch_publisher(root_uri, active_branch.as_deref(), dataset.session());
        let ManifestRows {
            tables,
            head,
            buffer,
            schema_contract,
            ..
        } = rows;
        Self {
            captured_contract: schema_contract.map(Arc::new),
            root_uri: root_uri.trim_end_matches('/').to_string(),
            dataset,
            known_state,
            active_branch,
            native_branch,
            branch_identifier,
            publisher,
            head: Arc::new(HistoryRecord {
                commit: head,
                tables,
            }),
            buffer: Arc::new(buffer),
            history: HistoryCache::default(),
        }
    }

    fn snapshot_from_state(root_uri: &str, state: ManifestState) -> Snapshot {
        Snapshot {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            version: state.version,
            entries: state
                .entries
                .into_iter()
                .map(|entry| (entry.type_key.clone(), entry))
                .collect(),
            schema_contract: state.schema_contract,
            contract_source: ContractSource::Manifest,
            captured_contract: None,
            manifest_dataset: None,
            graph_heads: state.graph_heads,
            graph_branch: None,
            native_branch: None,
            read_caches: None,
        }
    }

    #[cfg(test)]
    fn with_batch_publisher(mut self, publisher: Arc<dyn ManifestBatchPublisher>) -> Self {
        self.publisher = publisher;
        self
    }

    /// Test-only composition of the two init halves; production init goes
    /// through them separately so the commit point is a caller-visible
    /// boundary (issue #495).
    #[cfg(any(test, feature = "test-util"))]
    pub async fn init(root_uri: &str, catalog: &Catalog) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        let attempt = GenesisManifestAttempt::mint(catalog.system_columns)?;
        let contract = SchemaContractRow::for_test_catalog(catalog)?;
        let dataset =
            Self::init_commit(root_uri, catalog, &contract, &control_session, &attempt).await?;
        Self::finish_init(root_uri, dataset).await
    }

    /// Commit half of manifest init; ends at the `__manifest` Create commit
    /// (assembled in `init_manifest_graph`).
    #[doc(hidden)]
    pub async fn init_commit(
        root_uri: &str,
        catalog: &Catalog,
        contract: &SchemaContractRow,
        control_session: &Arc<lance::session::Session>,
        attempt: &GenesisManifestAttempt,
    ) -> std::result::Result<Dataset, ManifestInitError> {
        init_manifest_graph(
            root_uri.trim_end_matches('/'),
            catalog,
            contract,
            control_session,
            attempt,
        )
        .await
    }

    pub async fn read_schema_contract(&self) -> Result<SchemaContractRow> {
        if let Some(row) = &self.captured_contract
            && self.known_state.schema_contract.as_ref() == Some(&row.head)
        {
            return Ok((**row).clone());
        }
        if let Some(batch) = self.publisher.cached_rows(&self.dataset) {
            return state::schema_contract_from_batch(
                &self.dataset,
                &batch,
                self.known_state.schema_contract.as_ref(),
            );
        }
        read_schema_contract_row(&self.dataset, self.known_state.schema_contract.as_ref()).await
    }

    pub async fn read_schema_contract_for_snapshot(
        root_uri: &str,
        snapshot: &Snapshot,
    ) -> Result<SchemaContractRow> {
        if root_uri.trim_end_matches('/') != snapshot.root_uri {
            return Err(OmniError::manifest(
                "schema-contract snapshot belongs to another root",
            ));
        }
        if let Some(row) = &snapshot.captured_contract
            && snapshot.schema_contract() == Some(&row.head)
        {
            return Ok((**row).clone());
        }
        if let ContractSource::Archive(named) = &snapshot.contract_source {
            let (Some(digest), Some(head)) = (named, snapshot.schema_contract()) else {
                return Err(OmniError::manifest_internal(
                    "the commit of this snapshot recorded no schema content; its schema \
                     contract cannot be read",
                ));
            };
            let control_session = crate::lance_access::control_session();
            return history::read_schema(&snapshot.root_uri, &control_session, digest, head).await;
        }
        if let Some(dataset) = &snapshot.manifest_dataset {
            return read_schema_contract_row(dataset, snapshot.schema_contract()).await;
        }
        if snapshot.graph_branch().is_some() && snapshot.native_branch().is_none() {
            return Err(OmniError::manifest(
                "named schema-contract snapshot lacks native branch provenance",
            ));
        }
        let control_session = crate::lance_access::control_session();
        let dataset = open_manifest_dataset_native_with_session(
            root_uri.trim_end_matches('/'),
            snapshot.native_branch(),
            &control_session,
        )
        .await?;
        let dataset = dataset
            .checkout_version(snapshot.graph_manifest_version())
            .await
            .map_err(OmniError::storage)?;
        read_schema_contract_row(&dataset, snapshot.schema_contract()).await
    }

    pub async fn read_schema_contract_at(
        root_uri: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<SchemaContractRow> {
        let control_session = crate::lance_access::control_session();
        let dataset = open_manifest_dataset_with_session(
            root_uri.trim_end_matches('/'),
            branch,
            &control_session,
        )
        .await?;
        let dataset = dataset
            .checkout_version(version)
            .await
            .map_err(OmniError::storage)?;
        let state = read_manifest_state(&dataset).await?;
        read_schema_contract_row(&dataset, state.schema_contract.as_ref()).await
    }

    pub fn validate_serving_format(&self) -> Result<()> {
        migrations::refuse_if_stamp_unsupported(migrations::guard_stamp(&self.dataset)?)
    }

    pub async fn prepare_open_with_contract(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<PreparedManifestOpen> {
        let root = root_uri.trim_end_matches('/');
        let dataset = open_manifest_dataset_with_session(root, None, control_session).await?;
        migrations::refuse_if_stamp_unsupported(migrations::guard_stamp(&dataset)?)?;
        Ok(PreparedManifestOpen {
            root_uri: root.to_string(),
            dataset,
        })
    }

    pub async fn open_prepared_with_lineage_and_contract(
        root_uri: &str,
        prepared: PreparedManifestOpen,
    ) -> Result<(Self, Vec<GraphLineageRow>, Result<SchemaContractRow>)> {
        let root = root_uri.trim_end_matches('/');
        if prepared.root_uri != root {
            return Err(OmniError::manifest_internal(
                "prepared manifest belongs to a different graph root",
            ));
        }
        let dataset = prepared.dataset;
        let control_session = dataset.session();
        let captured_manifest = dataset.manifest().clone();
        let captured_location = dataset.manifest_location().clone();
        let captured = Box::pin(Self::project_opened_with_lineage(
            root,
            None,
            dataset,
            lance::dataset::refs::BranchIdentifier::main(),
            None,
            true,
        ))
        .await;
        let (coordinator, lineage, contract) = match captured {
            Ok(captured) => captured,
            Err(error) => {
                let current =
                    open_manifest_dataset_with_session(root, None, &control_session).await?;
                let location = current.manifest_location();
                if captured_location.path == location.path
                    && captured_location.version == location.version
                    && captured_location.naming_scheme == location.naming_scheme
                    && captured_location.e_tag == location.e_tag
                    && captured_location.size == location.size
                    && &captured_manifest == current.manifest()
                    && captured_manifest.schema.metadata == current.schema().metadata
                {
                    return Err(error);
                }
                migrations::refuse_if_stamp_unsupported(migrations::guard_stamp(&current)?)?;
                Box::pin(Self::project_opened_with_lineage(
                    root,
                    None,
                    current,
                    lance::dataset::refs::BranchIdentifier::main(),
                    None,
                    true,
                ))
                .await?
            }
        };
        Ok((
            coordinator,
            lineage,
            contract.expect("contract projection returns its complete schema row"),
        ))
    }

    async fn open_with_lineage_inner(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
        read_contract: bool,
    ) -> Result<(
        Self,
        Vec<GraphLineageRow>,
        Option<Result<SchemaContractRow>>,
    )> {
        let root = root_uri.trim_end_matches('/');
        let branch = branch.filter(|branch| *branch != "main");
        // Retain the fold accumulators alongside the state (the incremental merge-authority projection): the
        // scan this open pays anyway becomes the base a later refresh folds
        // deltas into, instead of a sunk cost repeated per refresh.
        let (dataset, branch_identifier, native_branch) =
            open_manifest_branch_with_identifier(root, branch, control_session).await?;
        Self::project_opened_with_lineage(
            root,
            branch,
            dataset,
            branch_identifier,
            native_branch,
            read_contract,
        )
        .await
    }

    pub async fn open_with_lineage_and_contract(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<(Self, Vec<GraphLineageRow>, Result<SchemaContractRow>)> {
        let (coordinator, lineage, contract) = Box::pin(Self::open_with_lineage_inner(
            root_uri,
            branch,
            control_session,
            true,
        ))
        .await?;
        Ok((
            coordinator,
            lineage,
            contract.expect("contract projection returns its complete schema row"),
        ))
    }

    async fn project_opened_with_lineage(
        root: &str,
        branch: Option<&str>,
        dataset: Dataset,
        branch_identifier: lance::dataset::refs::BranchIdentifier,
        native_branch: Option<String>,
        _read_contract: bool,
    ) -> Result<(
        Self,
        Vec<GraphLineageRow>,
        Option<Result<SchemaContractRow>>,
    )> {
        let (rows, contract) = state::read_manifest_rows_with_contract(&dataset).await?;
        let known_state = rows.state(dataset.version().version, native_branch.as_deref())?;
        let opened = OpenedManifest {
            dataset,
            known_state,
            rows,
            branch_identifier,
            native_branch,
        };
        let coordinator = Self::from_opened(root, opened, branch.map(str::to_string));
        Ok((coordinator, Vec::new(), Some(contract)))
    }

    /// Probe an acknowledgement-unknown manifest Create and accept only the
    /// exact immutable genesis receipt minted by this initialization attempt.
    /// A transport/read/mismatch error remains indeterminate to the caller;
    /// this method never turns absence or ambiguity into cleanup authority.
    pub async fn open_exact_genesis(
        root_uri: &str,
        attempt: &GenesisManifestAttempt,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let opened = open_exact_genesis_manifest(root, attempt, control_session).await?;
        Ok(Self::from_opened(root, opened, None))
    }

    /// Post-commit half of manifest init: reads the committed state back and
    /// assembles the coordinator; see `init_post_commit_checks` for the
    /// caller contract.
    pub async fn finish_init(root_uri: &str, dataset: Dataset) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let opened = load_initial_manifest_state(dataset).await?;
        Ok(Self::from_opened(root, opened, None))
    }

    /// Open an existing graph's manifest.
    #[doc(hidden)]
    pub async fn open(root_uri: &str) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        Self::open_with_session(root_uri, &control_session).await
    }

    pub async fn open_with_session(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        let root = root_uri.trim_end_matches('/');
        let opened = Box::pin(open_manifest_graph(root, None, control_session)).await?;
        Ok(Self::from_opened(root, opened, None))
    }

    /// Open an existing graph's manifest at a specific branch.
    pub async fn open_at_branch(root_uri: &str, branch: &str) -> Result<Self> {
        let control_session = crate::lance_access::control_session();
        Self::open_at_branch_with_session(root_uri, branch, &control_session).await
    }

    pub async fn open_at_branch_with_session(
        root_uri: &str,
        branch: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Self> {
        if branch == "main" {
            return Self::open_with_session(root_uri, control_session).await;
        }

        let root = root_uri.trim_end_matches('/');
        let opened = Box::pin(open_manifest_graph(root, Some(branch), control_session)).await?;
        Ok(Self::from_opened(root, opened, Some(branch.to_string())))
    }

    pub async fn snapshot_at(
        root_uri: &str,
        branch: Option<&str>,
        version: u64,
    ) -> Result<Snapshot> {
        let root = root_uri.trim_end_matches('/');
        let (dataset, state) = snapshot_state_at(root, branch, version).await?;
        let mut snapshot = Self::snapshot_from_state(root, state);
        snapshot.native_branch = dataset.manifest().branch.clone();
        snapshot.manifest_dataset = Some(dataset);
        snapshot.graph_branch = branch
            .filter(|branch| *branch != "main")
            .map(str::to_string);
        Ok(snapshot)
    }

    /// One live graph branch's `__manifest` opened once for the collector,
    /// under the cleanup control gates: every version, registration row and
    /// head the run compares for that branch comes from this open.
    pub async fn collector_branch_under_control_gates(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<CollectorBranch> {
        let (dataset, identifier, _native) =
            open_manifest_branch_with_identifier(root_uri, branch, control_session).await?;
        Ok(CollectorBranch {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            branch: branch
                .filter(|branch| *branch != "main")
                .map(str::to_string),
            dataset,
            identifier,
        })
    }

    /// Inventory registered table lifetimes, including soft-dropped tables, under cleanup's gates.
    pub async fn table_registrations_under_control_gates(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<Vec<TableRegistration>> {
        let main =
            open_manifest_dataset_native_with_session(root_uri, None, control_session).await?;
        state::read_manifest_table_registrations(&main).await
    }

    /// Return a Snapshot from the known manifest state. No storage I/O.
    pub fn snapshot(&self) -> Snapshot {
        let mut snapshot = Self::snapshot_from_state(&self.root_uri, self.known_state.clone());
        snapshot.native_branch = self.native_branch.clone();
        snapshot.graph_branch = self.active_branch.clone();
        snapshot.manifest_dataset = Some(self.dataset.clone());
        snapshot.captured_contract = self.captured_contract.clone();
        snapshot
    }

    pub fn control_session(&self) -> Arc<lance::session::Session> {
        self.dataset.session()
    }

    pub fn captured_probe(&self) -> CapturedManifestProbe {
        CapturedManifestProbe {
            dataset: self.dataset.clone(),
            active_branch: self.active_branch.clone(),
            captured: self.incarnation(),
        }
    }

    /// Install the latest version of the branch's `__manifest`: its table
    /// state and its head commit's record, from one scan of that version.
    /// Every fallible read completes before a field is replaced, so a failure
    /// leaves the previous view coherent. The scan leaves the schema contract
    /// content unread, except for a branch recreated since the held version:
    /// that is a new lifetime, captured with its contract as an open captures it.
    /// A root replaced at the held version is one too, and it resets the history
    /// cache in place, for every handle and coordinator sharing it: no commit
    /// or `__history` object held from the replaced root describes the new one.
    /// Main's native branch identifier and numeric version survive a replacement
    /// of the root, so the held view is kept only while the complete immutable
    /// manifest image still matches its held pin.
    pub async fn refresh(&mut self) -> Result<()> {
        Box::pin(self.refresh_inner()).await
    }

    /// The body of [`Self::refresh`], boxed there: it is awaited deep inside
    /// the merge future, the engine's known stack-depth hazard, and the box
    /// keeps its layout out of every caller's generator frame.
    async fn refresh_inner(&mut self) -> Result<()> {
        let control_session = self.dataset.session();
        let (dataset, branch_identifier, native_branch) = open_manifest_branch_with_identifier(
            &self.root_uri,
            self.active_branch.as_deref(),
            &control_session,
        )
        .await?;
        crate::instrumentation::record_projection_incremental_refresh();
        let recreated = branch_identifier != self.branch_identifier;
        let mut replaced_root = false;
        if !recreated && dataset.version().version == self.version() {
            let held_image_still_pinned = publisher::manifest_image_matches(
                self.dataset.manifest(),
                self.dataset.manifest_location(),
                &dataset,
            );
            if held_image_still_pinned {
                return Ok(());
            }
            tracing::debug!(
                "manifest refresh: manifest image changed at the held version; root replaced, full read"
            );
            replaced_root = true;
        }
        let rows = match recreated || replaced_root {
            true => state::read_manifest_rows_with_contract(&dataset).await?.0,
            false => state::read_manifest_rows_projected(&dataset).await?,
        };
        let known_state = rows.state(dataset.version().version, native_branch.as_deref())?;
        fail(&READ_REFRESH_POST_STATE_PRE_LINEAGE)?;
        self.dataset = dataset;
        self.known_state = known_state;
        self.branch_identifier = branch_identifier;
        self.native_branch = native_branch;
        if replaced_root {
            self.history.reset();
        }
        self.install_head(rows, Vec::new());
        Ok(())
    }

    /// Replace the head and the buffer with those of `rows`. The commits this
    /// coordinator held and `rows` does not were appended to `__history`, and
    /// `merged` was unless `rows` holds it: they enter the history cache, oldest first.
    fn install_head(&mut self, rows: ManifestRows, merged: Vec<GraphLineageRow>) {
        let ManifestRows {
            tables,
            head,
            buffer,
            schema_contract,
            ..
        } = rows;
        self.captured_contract = schema_contract.map(Arc::new);
        let head = HistoryRecord {
            commit: head,
            tables,
        };
        let replaced = std::mem::replace(&mut self.head, Arc::new(head));
        let previous = std::mem::replace(&mut self.buffer, Arc::new(buffer));
        let left = previous
            .commits()
            .iter()
            .chain(std::iter::once(&replaced.commit))
            .filter(|commit| self.held_commit(&commit.graph_commit_id).is_none())
            .cloned();
        self.history.settle(
            left.chain(merged)
                .map(commit_graph::graph_commit_from_manifest_row),
        );
    }

    /// The head commit's record, from the `__manifest` version
    /// [`Self::snapshot`] was read at: the branch's effective head, which a
    /// fork that has not published inherited from its source.
    pub fn head(&self) -> &GraphLineageRow {
        &self.head.commit
    }

    /// The head commit's record with the `table` rows of the `__manifest`
    /// version that holds it.
    pub fn head_record(&self) -> &HistoryRecord {
        &self.head
    }

    /// The buffer of the `__manifest` version [`Self::snapshot`] was read at.
    pub fn buffer(&self) -> &CommitBuffer {
        &self.buffer
    }

    /// The head or the buffered commit `graph_commit_id`, with no request.
    pub fn held_commit(&self, graph_commit_id: &str) -> Option<&GraphLineageRow> {
        if self.head.commit.graph_commit_id == graph_commit_id {
            return Some(&self.head.commit);
        }
        self.buffer.get(graph_commit_id)
    }

    /// The head or the buffered commit that `requested` names by its published
    /// ID or by its intent nonce; two held commits answering one request is a
    /// malformed manifest, refused as `requested_commit` refuses it.
    pub fn held_commit_answering(&self, requested: &str) -> Result<Option<&GraphLineageRow>> {
        let mut answering = None;
        for commit in std::iter::once(&self.head.commit).chain(self.buffer.commits()) {
            if commit_id_answers(&commit.graph_commit_id, requested)? {
                if answering.is_some() {
                    return Err(OmniError::manifest_internal(format!(
                        "two held commits answer the requested commit id {requested}"
                    )));
                }
                answering = Some(commit);
            }
        }
        Ok(answering)
    }

    /// The record of the head or of the buffered commit `graph_commit_id`,
    /// with the `table` rows of the version that wrote it and no request.
    pub fn held_record(&self, graph_commit_id: &str) -> Option<HistoryRecord> {
        if self.head.commit.graph_commit_id == graph_commit_id {
            return Some(self.head.as_ref().clone());
        }
        self.buffer
            .get(graph_commit_id)
            .map(|commit| self.buffer.record_of(commit, &self.head.tables))
    }

    /// The records of the buffered commits, oldest first, and of the head:
    /// what a merge of this branch appends to `__history`.
    pub fn branch_records(&self) -> BranchRecords {
        BranchRecords {
            buffer: self.buffer.as_ref().clone(),
            head: self.head.as_ref().clone(),
            merge_base: None,
        }
    }

    /// The commit graph of the head and the buffer this coordinator holds.
    pub fn commit_graph(&self) -> CommitGraph {
        CommitGraph::from_head(
            &self.root_uri,
            self.dataset.session(),
            self.head.commit.clone(),
            self.buffer.commits(),
            self.history.clone(),
        )
    }

    pub fn history(&self) -> &HistoryCache {
        &self.history
    }

    /// Read settled commits through `history`, the cache of the handle that
    /// opened this coordinator.
    pub fn share_history(&mut self, history: HistoryCache) {
        self.history = history;
    }

    /// Commit updated sub-table versions to the manifest.
    ///
    /// Atomically replaces the pin of each updated table.
    /// The overwrite commit on `__manifest` is the graph-level publish point.
    #[cfg(test)]
    pub async fn commit(&mut self, updates: &[DatasetUpdate]) -> Result<u64> {
        let changes = updates
            .iter()
            .cloned()
            .map(ManifestChange::Update)
            .collect::<Vec<_>>();
        self.commit_changes(&changes).await
    }

    /// Same as [`commit`], but with caller-supplied per-table expected
    /// versions used for optimistic concurrency control. Each entry asserts
    /// the manifest's current latest non-tombstoned `table_version` for that
    /// table identity is exactly what the caller observed; mismatches surface
    /// as `OmniError::Manifest` with `ManifestConflictDetails::PublishedDatasetVersionMismatch`.
    #[cfg(test)]
    pub async fn commit_with_expected(
        &mut self,
        updates: &[DatasetUpdate],
        expected_table_versions: &ExpectedTableVersions,
    ) -> Result<u64> {
        let changes = updates
            .iter()
            .cloned()
            .map(ManifestChange::Update)
            .collect::<Vec<_>>();
        self.commit_changes_with_expected(&changes, expected_table_versions)
            .await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn commit_changes(&mut self, changes: &[ManifestChange]) -> Result<u64> {
        self.commit_changes_with_expected(changes, &HashMap::new())
            .await
    }

    #[cfg(any(test, feature = "test-util"))]
    pub async fn commit_changes_with_expected(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
    ) -> Result<u64> {
        Ok(self
            .commit_changes_with_lineage(changes, expected_table_versions, None)
            .await?
            .version)
    }

    /// Publish `changes` and, when `lineage` is present, record the graph commit
    /// in the SAME overwrite (RFC-013 Phase 7): the commit's record rides the
    /// publish of the `table` rows, so the whole commit lands at one manifest
    /// version — no separate write, no manifest→commit-graph atomicity gap, no
    /// per-write commit-graph refresh. Returns the new version and the record
    /// the publisher wrote for the commit (so the caller can update its
    /// in-memory commit cache without a re-read). Without `lineage` the head
    /// commit's record stays as it is while the `table` rows move, which no
    /// production writer does.
    #[cfg(any(test, feature = "test-util"))]
    pub async fn commit_changes_with_lineage(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
    ) -> Result<CommitOutcome> {
        self.commit_changes_with_lineage_and_precondition(
            changes,
            expected_table_versions,
            lineage,
            &PublishPrecondition::Any,
        )
        .await
    }

    /// Token-aware graph publication. Exact authority is checked by the
    /// publisher from every CAS attempt's existing one-scan state.
    #[doc(hidden)]
    pub async fn commit_changes_with_lineage_and_precondition(
        &mut self,
        changes: &[ManifestChange],
        expected_table_versions: &ExpectedTableVersions,
        lineage: Option<&LineageIntent>,
        precondition: &PublishPrecondition,
    ) -> Result<CommitOutcome> {
        if changes.is_empty()
            && expected_table_versions.is_empty()
            && lineage.is_none()
            && matches!(precondition, PublishPrecondition::Any)
        {
            return Ok(CommitOutcome {
                version: self.version(),
                parent_commit_id: None,
                commit: None,
            });
        }

        let PublishOutcome {
            dataset,
            parent_commit_id,
            known_state,
            head,
            tables,
            buffer,
            schema_contract,
        } = self
            .publisher
            .publish_with_precondition(changes, expected_table_versions, lineage, precondition)
            .await?;
        self.dataset = dataset;
        self.known_state = known_state;
        let recorded = parent_commit_id.is_some();
        let merged = lineage
            .filter(|_| recorded)
            .and_then(|intent| intent.merged_parent.as_ref())
            .into_iter()
            .flat_map(BranchRecords::commits)
            .cloned()
            .collect();
        let rows = ManifestRows {
            schema_contract_head: schema_contract.as_ref().map(|row| row.head.clone()),
            tables,
            head: head.clone(),
            buffer,
            schema_contract,
        };
        self.install_head(rows, merged);
        Ok(CommitOutcome {
            version: self.version(),
            parent_commit_id,
            commit: recorded.then_some(head),
        })
    }

    /// The head commit's record of `branch`, from the latest version of its
    /// `__manifest`.
    pub async fn read_head_at(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<GraphLineageRow> {
        let dataset = open_manifest_dataset_with_session(root_uri, branch, control_session).await?;
        Ok(state::read_manifest_rows_projected(&dataset).await?.head)
    }

    /// The head commit's record of `branch` and the buffer beside it, from the
    /// latest version of its `__manifest`.
    pub async fn read_held_at(
        root_uri: &str,
        branch: Option<&str>,
        control_session: &Arc<lance::session::Session>,
    ) -> Result<(GraphLineageRow, CommitBuffer)> {
        let dataset = open_manifest_dataset_with_session(root_uri, branch, control_session).await?;
        let rows = state::read_manifest_rows_projected(&dataset).await?;
        Ok((rows.head, rows.buffer))
    }

    /// The record of `graph_commit_id` from the latest `__manifest` version of
    /// a live branch that holds it as its head or in its buffer: where a
    /// commit is found while `__history` does not hold it yet. One open of
    /// `__manifest`, one listing of its branches, and one scan per branch
    /// until the commit is found: the branch of this coordinator, then main,
    /// then the other live branches, the one created last first.
    pub async fn record_in_live_branches(
        &self,
        graph_commit_id: &str,
    ) -> Result<Option<HistoryRecord>> {
        let session = self.dataset.session();
        let main = open_manifest_dataset_with_session(&self.root_uri, None, &session).await?;
        let mut others: Vec<(u64, Option<String>, String)> =
            list_live_manifest_branch_contents(&main)
                .await?
                .into_iter()
                .filter(|(native, _)| {
                    native != "main" && Some(native) != self.native_branch.as_ref()
                })
                .map(|(native, contents)| {
                    let (_, incarnation) = crate::branch_names::split_native_branch_name(&native);
                    let incarnation = incarnation.map(str::to_string);
                    (contents.create_at, incarnation, native)
                })
                .collect();
        others.sort_by(|a, b| b.cmp(a));
        let own = self.native_branch.clone().map(Some);
        let natives = own
            .into_iter()
            .chain(std::iter::once(None))
            .chain(others.into_iter().map(|(_, _, native)| Some(native)));
        for native in natives {
            let branch = match &native {
                None => main.clone(),
                Some(native) => match main.checkout_branch(native).await {
                    Ok(branch) => branch,
                    Err(lance::Error::RefNotFound { .. }) => continue,
                    Err(error) => return Err(OmniError::storage(error)),
                },
            };
            let rows = state::read_manifest_rows_projected(&branch).await?;
            if let Some(record) = rows.record(graph_commit_id) {
                return Ok(Some(record));
            }
        }
        Ok(None)
    }

    /// Refuse `commit` unless the branch incarnation that wrote it is the live
    /// incarnation of its branch. The tables a commit of a retired incarnation
    /// names are the collector's to reclaim, and a recreated branch reuses the
    /// name and the version numbers of the one it replaces.
    pub async fn ensure_incarnation_live(
        root_uri: &str,
        control_session: &Arc<lance::session::Session>,
        commit: &GraphLineageRow,
    ) -> Result<()> {
        let (Some(native), Some(branch)) = (&commit.native_branch, &commit.graph_branch) else {
            return Ok(());
        };
        let main = open_manifest_dataset_with_session(root_uri, None, control_session).await?;
        let live = resolve_native_manifest_branch(&main, branch).await?;
        if &live != native {
            return Err(OmniError::manifest(format!(
                "commit '{}' has no persisted native-branch incarnation witness: branch \
                 '{branch}' was deleted and recreated after the commit was written",
                commit.graph_commit_id
            )));
        }
        Ok(())
    }

    /// Current graph-manifest version.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    #[cfg(test)]
    pub async fn probe_latest_version(&self) -> Result<u64> {
        self.dataset
            .latest_version_id()
            .await
            .map_err(OmniError::storage)
    }

    /// Lance-native stable identity captured with the active manifest state.
    /// Unlike a manifest version/eTag, this remains stable across ordinary
    /// commits and changes when a named branch is deleted and recreated (ABA
    /// protection). Returning the capture, rather than re-reading the live ref,
    /// prevents callers from pairing old state with a replacement witness.
    pub async fn branch_identifier(&self) -> Result<lance::dataset::refs::BranchIdentifier> {
        Ok(self.branch_identifier.clone())
    }

    /// The native Lance ref this coordinator's branch resolved to; `None` on main.
    pub fn native_branch(&self) -> Option<&str> {
        self.native_branch.as_deref()
    }

    /// Exact head of the active branch from the same pinned
    /// manifest version as [`Self::snapshot`]. This is write authority, not a
    /// lineage-cache query: a read may refresh only the manifest, so consulting
    /// `CommitGraph` here would combine a fresh table snapshot with a stale head.
    pub fn exact_graph_head(&self) -> Option<String> {
        let branch_key = self
            .active_branch
            .as_deref()
            .unwrap_or(MAIN_BRANCH_HEAD_KEY);
        self.known_state.graph_heads.get(branch_key).cloned()
    }

    pub fn incarnation(&self) -> ManifestIncarnation {
        ManifestIncarnation {
            version: self.version(),
            e_tag: self.dataset.manifest_location().e_tag.clone(),
            timestamp_nanos: Some(self.dataset.manifest().timestamp_nanos),
            branch_identifier: self.branch_identifier.clone(),
        }
    }

    /// Latest committed manifest identity. Main cannot be deleted/recreated, so
    /// the cheap version-number probe is sufficient there. Non-main Lance
    /// branches can be deleted and recreated with the same version, e_tag, and
    /// timestamp when both lifetimes fork the same source; the native branch
    /// identifier is therefore part of the freshness result as well.
    pub async fn probe_latest_incarnation(&self) -> Result<ManifestIncarnation> {
        probe_dataset_latest_incarnation(&self.dataset, self.active_branch.as_deref()).await
    }

    /// Create the logical branch `name` as a fresh native incarnation.
    ///
    /// The native ref is `{name}.{ulid}`, so a recreated branch never shares a
    /// physical path with a dead predecessor; the logical name stays the only
    /// public identity. The registry check runs on logical names, so a live
    /// legacy ref named exactly `name` also counts as existing.
    #[doc(hidden)]
    pub async fn create_branch(&mut self, name: &str) -> Result<()> {
        crate::branch_names::ensure_logical_branch_name(name)?;
        let mut ds = self.dataset.clone();
        crate::branch_control::ensure_manifest_branch_create_namespace(&ds, name)
            .await
            .map_err(OmniError::before_effect)?;
        let native =
            crate::branch_names::native_branch_name(name, &crate::branch_names::mint_incarnation());
        match crate::branch_control::create_branch_recoverably(&mut ds, &native, self.version())
            .await?
        {
            crate::branch_control::BranchCreateOutcome::Created => Ok(()),
            crate::branch_control::BranchCreateOutcome::RefAlreadyExists => Err(
                OmniError::manifest_conflict(format!("branch '{}' already exists", name)),
            ),
        }
    }

    async fn open_branch_control_dataset(&self) -> Result<Dataset> {
        let uri = manifest_uri(&self.root_uri);
        crate::instrumentation::open_dataset(
            &uri,
            crate::instrumentation::VersionResolution::Latest,
            None,
            crate::instrumentation::manifest_wrapper(),
        )
        .await
    }

    /// Append the commits the branch `native` wrote, of those it buffers and
    /// its head, to `__history` as one Append, before its ref is retired. The
    /// commits it inherited stay with the branch that wrote them; a retry appends copies.
    async fn settle_head_of(&self, control: &Dataset, native: &str, logical: &str) -> Result<()> {
        let records = match self.native_branch.as_deref() == Some(native) {
            true => self.branch_records(),
            false => {
                let branch = control
                    .checkout_branch(native)
                    .await
                    .map_err(|error| branch_ref_error(error, logical))?;
                state::read_manifest_rows_projected(&branch)
                    .await?
                    .records()
            }
        };
        let mut closed = history::ClosedBlocks::default();
        history::closed_blocks(records.commits(), &mut closed)?;
        let records: Vec<_> = records
            .held()
            .filter(|held| held.commit.native_branch.as_deref() == Some(native))
            .collect();
        if records.is_empty() {
            return Ok(());
        }
        history::settle_closed(&self.root_uri, &self.dataset.session(), &records, &closed).await?;
        Ok(())
    }

    #[doc(hidden)]
    pub async fn delete_branch(&mut self, name: &str) -> Result<()> {
        let ds = self.open_branch_control_dataset().await?;
        let branches = list_live_manifest_branch_contents(&ds).await?;
        let native =
            crate::branch_names::resolve_native_branch(branches.keys().map(String::as_str), name)?
                .ok_or_else(|| {
                    OmniError::manifest_not_found(format!("branch '{}' not found", name))
                })?;
        let expected_identifier = branches
            .get(&native)
            .ok_or_else(|| OmniError::manifest_not_found(format!("branch '{}' not found", name)))?
            .identifier
            .clone();
        self.settle_head_of(&ds, &native, name).await?;
        crate::branch_control::retire_branch_recoverably(&ds, &native, &expected_identifier)
            .await
            // Legacy control is also used to finish a schema apply. Its
            // caller may already have effects, so an inner refusal cannot
            // certify the whole operation as effect-free.
            .map_err(OmniError::without_pre_effect_evidence)?;
        Ok(())
    }

    /// Delete `name` only if its live Lance BranchContents still has the exact
    /// identifier captured by the caller's post-gate branch view.
    ///
    /// The coordinator may itself be bound to `name`: operation-local native
    /// controls discard that captured coordinator after the authority change,
    /// so refreshing a just-deleted bound ref would be both wasted work and an
    /// error. Deleting a sibling ref does not mutate this coordinator's pinned
    /// manifest version/state either.
    #[doc(hidden)]
    pub async fn delete_branch_with_expected(
        &mut self,
        name: &str,
        expected_identifier: &lance::dataset::refs::BranchIdentifier,
    ) -> Result<()> {
        let ds = self
            .open_branch_control_dataset()
            .await
            .map_err(OmniError::before_effect)?;
        let native = resolve_native_manifest_branch(&ds, name)
            .await
            .map_err(OmniError::before_effect)?;
        self.settle_head_of(&ds, &native, name).await?;
        crate::branch_control::retire_branch_recoverably(&ds, &native, expected_identifier).await
    }

    /// Logical graph branches, `main` first. Each live native ref maps to
    /// exactly one logical name; a duplicate incarnation fails loudly.
    pub async fn list_graph_branches(&self) -> Result<Vec<String>> {
        let branches = list_live_manifest_branch_contents(&self.dataset).await?;
        let mut names = Vec::with_capacity(branches.len());
        let mut seen = HashSet::with_capacity(branches.len());
        for native in branches.keys().filter(|name| *name != "main") {
            let logical = crate::branch_names::logical_branch_name(native).to_string();
            if !seen.insert(logical.clone()) {
                return Err(OmniError::manifest_conflict(format!(
                    "branch '{logical}' has more than one live native incarnation; run cleanup \
                     before using it"
                )));
            }
            names.push(logical);
        }
        names.sort();
        let mut all = vec!["main".to_string()];
        all.extend(names);
        Ok(all)
    }
}

#[cfg(test)]
mod tests;
