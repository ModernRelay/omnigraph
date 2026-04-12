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
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use lance_namespace::NamespaceError;
use lance_namespace::models::CreateTableVersionRequest;

use crate::error::{OmniError, Result};

use super::layout::{open_manifest_dataset, tombstone_object_id, version_object_id};
use super::metadata::parse_namespace_version_request;
use super::state::{
    manifest_rows_batch, manifest_schema, read_manifest_entries, read_registered_table_locations,
    read_tombstone_versions,
};
use super::{
    ManifestChange, OBJECT_TYPE_TABLE, OBJECT_TYPE_TABLE_TOMBSTONE, OBJECT_TYPE_TABLE_VERSION,
    SubTableEntry, SubTableUpdate, TableRegistration, TableTombstone,
};

#[async_trait]
pub(super) trait ManifestBatchPublisher: Send + Sync {
    async fn publish(&self, changes: &[ManifestChange]) -> Result<Dataset>;
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

    async fn load_publish_state(
        &self,
    ) -> Result<(
        Dataset,
        HashMap<String, String>,
        HashMap<(String, u64), SubTableEntry>,
        HashMap<(String, u64), ()>,
    )> {
        let dataset = self.dataset().await?;
        let registered_tables = read_registered_table_locations(&dataset).await?;
        let existing_entries = read_manifest_entries(&dataset).await?;
        let existing_versions = existing_entries
            .iter()
            .map(|entry| {
                (
                    (entry.table_key.clone(), entry.table_version),
                    entry.clone(),
                )
            })
            .collect();
        let existing_tombstones = read_tombstone_versions(&dataset).await?;
        Ok((
            dataset,
            registered_tables,
            existing_versions,
            existing_tombstones,
        ))
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

    async fn merge_rows(&self, dataset: Dataset, rows: Vec<PendingVersionRow>) -> Result<Dataset> {
        let batch = Self::pending_rows_to_batch(rows)?;
        let reader = RecordBatchIterator::new(vec![Ok(batch)], manifest_schema());
        let dataset = Arc::new(dataset);
        let mut merge_builder = MergeInsertBuilder::try_new(dataset, vec!["object_id".to_string()])
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        merge_builder.when_matched(WhenMatched::UpdateAll);
        merge_builder.when_not_matched(WhenNotMatched::InsertAll);
        merge_builder.conflict_retries(5);
        merge_builder.use_index(false);
        let (new_dataset, _stats) = merge_builder
            .try_build()
            .map_err(|e| OmniError::Lance(e.to_string()))?
            .execute_reader(Box::new(reader))
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?;
        Ok(Arc::try_unwrap(new_dataset).unwrap_or_else(|arc| (*arc).clone()))
    }

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
        self.publish(&changes).await
    }
}

#[async_trait]
impl ManifestBatchPublisher for GraphNamespacePublisher {
    async fn publish(&self, changes: &[ManifestChange]) -> Result<Dataset> {
        if changes.is_empty() {
            return self.dataset().await;
        }

        let (dataset, known_tables, existing_versions, existing_tombstones) =
            self.load_publish_state().await?;
        let rows = Self::build_pending_rows(
            changes,
            &known_tables,
            &existing_versions,
            &existing_tombstones,
        )?;
        self.merge_rows(dataset, rows).await
    }
}
