use std::sync::Arc;

use async_trait::async_trait;
use lance::Dataset;
use lance::dataset::builder::DatasetBuilder;
use lance_namespace::models::{
    CreateTableVersionRequest, CreateTableVersionResponse, DescribeTableRequest,
    DescribeTableResponse, DescribeTableVersionRequest, DescribeTableVersionResponse,
    ListTableVersionsRequest, ListTableVersionsResponse, TableExistsRequest, TableVersion,
};
use lance_namespace::{Error as LanceNamespaceError, LanceNamespace, NamespaceError};
use lance_table::io::commit::ManifestNamingScheme;
use object_store::{Error as ObjectStoreError, ObjectStore as _, PutMode, PutOptions, path::Path};

use crate::error::{OmniError, Result};

use super::layout::{
    namespace_internal_error, open_manifest_dataset, table_id_to_key, table_uri_for_path,
};
use super::metadata::{
    TableVersionMetadata, namespace_version_metadata, parse_namespace_version_request,
};
use super::publisher::GraphNamespacePublisher;
use super::state::{ManifestState, SubTableEntry, read_manifest_entries, read_manifest_state};

#[derive(Debug, Clone)]
struct BranchManifestNamespace {
    root_uri: String,
    branch: Option<String>,
}

impl BranchManifestNamespace {
    fn new(root_uri: &str, branch: Option<&str>) -> Self {
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

    async fn state(&self) -> Result<ManifestState> {
        let dataset = self.dataset().await?;
        read_manifest_state(&dataset).await
    }

    async fn version_entries(&self) -> Result<Vec<SubTableEntry>> {
        let dataset = self.dataset().await?;
        read_manifest_entries(&dataset).await
    }
}

#[derive(Debug, Clone)]
struct StagedTableNamespace {
    root_uri: String,
    table_id: Vec<String>,
    table_path: String,
    branch: Option<String>,
}

impl StagedTableNamespace {
    fn new(root_uri: &str, table_key: &str, table_path: &str, branch: Option<&str>) -> Self {
        Self {
            root_uri: root_uri.trim_end_matches('/').to_string(),
            table_id: vec![table_key.to_string()],
            table_path: table_path.to_string(),
            branch: branch
                .filter(|branch| *branch != "main")
                .map(ToOwned::to_owned),
        }
    }

    fn table_key(&self) -> &str {
        &self.table_id[0]
    }

    fn table_uri(&self) -> String {
        table_uri_for_path(&self.root_uri, &self.table_path, self.branch.as_deref())
    }

    fn ensure_request_table(
        &self,
        request_id: Option<&Vec<String>>,
    ) -> lance_namespace::Result<()> {
        match request_id {
            Some(request_id) if request_id == &self.table_id => Ok(()),
            Some(request_id) => Err(LanceNamespaceError::namespace_source(Box::new(
                NamespaceError::TableNotFound {
                    message: format!("table {:?} not found", request_id),
                },
            ))),
            None => Err(LanceNamespaceError::invalid_input("table id is required")),
        }
    }

    async fn open_head(&self) -> Result<Dataset> {
        Dataset::open(&self.table_uri())
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))
    }

    async fn open_version(&self, version: u64) -> Result<Dataset> {
        let ds = self.open_head().await?;
        if ds.version().version == version {
            Ok(ds)
        } else {
            ds.checkout_version(version)
                .await
                .map_err(|e| OmniError::Lance(e.to_string()))
        }
    }

    fn to_table_version(
        &self,
        dataset: &Dataset,
        version: &lance::dataset::Version,
    ) -> Result<TableVersion> {
        let metadata =
            TableVersionMetadata::from_dataset(&self.root_uri, &self.table_path, dataset)?;
        Ok(metadata.to_namespace_version_with_details(
            version.version,
            Some(version.timestamp.timestamp_millis()),
            Some(
                version
                    .metadata
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        ))
    }
}

pub(crate) fn branch_manifest_namespace(
    root_uri: &str,
    branch: Option<&str>,
) -> Arc<dyn LanceNamespace> {
    Arc::new(BranchManifestNamespace::new(root_uri, branch))
}

pub(crate) fn staged_table_namespace(
    root_uri: &str,
    table_key: &str,
    table_path: &str,
    branch: Option<&str>,
) -> Arc<dyn LanceNamespace> {
    Arc::new(StagedTableNamespace::new(
        root_uri, table_key, table_path, branch,
    ))
}

async fn load_table_from_namespace(
    namespace: Arc<dyn LanceNamespace>,
    table_key: &str,
    branch: Option<&str>,
    version: Option<u64>,
) -> Result<Dataset> {
    let builder = DatasetBuilder::from_namespace(namespace, vec![table_key.to_string()])
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let builder = match (branch, version) {
        (Some(branch), version) => builder.with_branch(branch, version),
        (None, Some(version)) => builder.with_version(version),
        (None, None) => builder,
    };
    builder
        .load()
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))
}

pub(crate) async fn open_table_at_version_from_manifest(
    root_uri: &str,
    table_key: &str,
    branch: Option<&str>,
    version: u64,
) -> Result<Dataset> {
    load_table_from_namespace(
        branch_manifest_namespace(root_uri, branch),
        table_key,
        branch,
        Some(version),
    )
    .await
}

#[async_trait]
impl LanceNamespace for BranchManifestNamespace {
    fn namespace_id(&self) -> String {
        "__manifest".to_string()
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> lance_namespace::Result<DescribeTableResponse> {
        let table_key = table_id_to_key(request.id.as_ref())?;
        let state = self
            .state()
            .await
            .map_err(|e| LanceNamespaceError::namespace_source(Box::new(e)))?;
        let entry = state
            .entries
            .into_iter()
            .find(|entry| entry.table_key == table_key);
        let entry = entry.ok_or_else(|| {
            LanceNamespaceError::namespace_source(Box::new(NamespaceError::TableNotFound {
                message: format!("table {} not found", table_key),
            }))
        })?;
        let table_uri =
            table_uri_for_path(&self.root_uri, &entry.table_path, self.branch.as_deref());

        Ok(DescribeTableResponse {
            table: Some(entry.table_key.clone()),
            namespace: Some(Vec::new()),
            version: Some(entry.table_version as i64),
            location: Some(table_uri.clone()),
            table_uri: request.with_table_uri.unwrap_or(false).then_some(table_uri),
            schema: None,
            storage_options: None,
            stats: None,
            metadata: None,
            properties: None,
            managed_versioning: Some(true),
        })
    }

    async fn table_exists(&self, request: TableExistsRequest) -> lance_namespace::Result<()> {
        let table_key = table_id_to_key(request.id.as_ref())?;
        let state = self
            .state()
            .await
            .map_err(|e| LanceNamespaceError::namespace_source(Box::new(e)))?;
        if state
            .entries
            .iter()
            .any(|entry| entry.table_key == table_key)
        {
            Ok(())
        } else {
            Err(LanceNamespaceError::namespace_source(Box::new(
                NamespaceError::TableNotFound {
                    message: format!("table {} not found", table_key),
                },
            )))
        }
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> lance_namespace::Result<ListTableVersionsResponse> {
        let table_key = table_id_to_key(request.id.as_ref())?;
        let mut versions: Vec<TableVersion> = self
            .version_entries()
            .await
            .map_err(|e| LanceNamespaceError::namespace_source(Box::new(e)))?
            .into_iter()
            .filter(|entry| entry.table_key == table_key)
            .map(|entry| {
                entry
                    .version_metadata
                    .to_namespace_version(entry.table_version)
            })
            .collect();

        if request.descending.unwrap_or(false) {
            versions.sort_by(|a, b| b.version.cmp(&a.version));
        } else {
            versions.sort_by(|a, b| a.version.cmp(&b.version));
        }
        if let Some(limit) = request.limit {
            versions.truncate(limit as usize);
        }

        Ok(ListTableVersionsResponse {
            versions,
            page_token: None,
        })
    }

    async fn describe_table_version(
        &self,
        request: DescribeTableVersionRequest,
    ) -> lance_namespace::Result<DescribeTableVersionResponse> {
        let table_key = table_id_to_key(request.id.as_ref())?;
        let version = request
            .version
            .ok_or_else(|| LanceNamespaceError::invalid_input("table version is required"))?;
        let version = u64::try_from(version).map_err(|_| {
            LanceNamespaceError::invalid_input("table version must be non-negative")
        })?;
        let entry = self
            .version_entries()
            .await
            .map_err(|e| LanceNamespaceError::namespace_source(Box::new(e)))?
            .into_iter()
            .find(|entry| entry.table_key == table_key && entry.table_version == version)
            .ok_or_else(|| {
                LanceNamespaceError::namespace_source(Box::new(
                    NamespaceError::TableVersionNotFound {
                        message: format!("table version {} not found for {}", version, table_key),
                    },
                ))
            })?;

        Ok(DescribeTableVersionResponse::new(
            entry
                .version_metadata
                .to_namespace_version(entry.table_version),
        ))
    }

    async fn create_table_version(
        &self,
        request: CreateTableVersionRequest,
    ) -> lance_namespace::Result<CreateTableVersionResponse> {
        let (table_key, table_version, row_count, table_branch, version_metadata) =
            parse_namespace_version_request(&request)?;
        GraphNamespacePublisher::new(&self.root_uri, self.branch.as_deref())
            .publish_requests(std::slice::from_ref(&request))
            .await
            .map_err(|e| LanceNamespaceError::namespace_source(Box::new(e)))?;
        let mut response = CreateTableVersionResponse::new();
        response.version = Some(Box::new(
            version_metadata.to_namespace_version_with_details(
                table_version,
                None,
                Some(namespace_version_metadata(
                    row_count,
                    table_branch.as_deref(),
                )),
            ),
        ));
        let _ = table_key;
        Ok(response)
    }
}

#[async_trait]
impl LanceNamespace for StagedTableNamespace {
    fn namespace_id(&self) -> String {
        "__manifest".to_string()
    }

    async fn describe_table(
        &self,
        request: DescribeTableRequest,
    ) -> lance_namespace::Result<DescribeTableResponse> {
        self.ensure_request_table(request.id.as_ref())?;
        let ds = self
            .open_head()
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        let table_uri = self.table_uri();
        Ok(DescribeTableResponse {
            table: Some(self.table_key().to_string()),
            namespace: Some(Vec::new()),
            version: Some(ds.version().version as i64),
            location: Some(table_uri.clone()),
            table_uri: request.with_table_uri.unwrap_or(false).then_some(table_uri),
            schema: None,
            storage_options: None,
            stats: None,
            metadata: None,
            properties: None,
            managed_versioning: Some(true),
        })
    }

    async fn table_exists(&self, request: TableExistsRequest) -> lance_namespace::Result<()> {
        self.ensure_request_table(request.id.as_ref())?;
        self.open_head()
            .await
            .map(|_| ())
            .map_err(|e| LanceNamespaceError::namespace_source(Box::new(e)))
    }

    async fn list_table_versions(
        &self,
        request: ListTableVersionsRequest,
    ) -> lance_namespace::Result<ListTableVersionsResponse> {
        self.ensure_request_table(request.id.as_ref())?;
        if request.limit == Some(0) {
            return Ok(ListTableVersionsResponse {
                versions: Vec::new(),
                page_token: None,
            });
        }
        let head = self
            .open_head()
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        let dataset_versions = head
            .versions()
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        let mut versions = Vec::with_capacity(dataset_versions.len());
        for version in dataset_versions {
            let dataset = if version.version == head.version().version {
                head.clone()
            } else {
                head.checkout_version(version.version)
                    .await
                    .map_err(|e| namespace_internal_error(e.to_string()))?
            };
            versions.push(
                self.to_table_version(&dataset, &version)
                    .map_err(|e| namespace_internal_error(e.to_string()))?,
            );
        }
        if request.descending.unwrap_or(false) {
            versions.sort_by(|a, b| b.version.cmp(&a.version));
        } else {
            versions.sort_by(|a, b| a.version.cmp(&b.version));
        }
        if let Some(limit) = request.limit {
            versions.truncate(limit as usize);
        }
        Ok(ListTableVersionsResponse {
            versions,
            page_token: None,
        })
    }

    async fn describe_table_version(
        &self,
        request: DescribeTableVersionRequest,
    ) -> lance_namespace::Result<DescribeTableVersionResponse> {
        self.ensure_request_table(request.id.as_ref())?;
        let version = request
            .version
            .ok_or_else(|| LanceNamespaceError::invalid_input("table version is required"))?;
        let version = u64::try_from(version).map_err(|_| {
            LanceNamespaceError::invalid_input("table version must be non-negative")
        })?;
        let ds = self
            .open_version(version)
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        let version_info = self
            .to_table_version(
                &ds,
                &lance::dataset::Version {
                    version: ds.version().version,
                    timestamp: ds.version().timestamp,
                    metadata: ds.version().metadata,
                },
            )
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        Ok(DescribeTableVersionResponse::new(version_info))
    }

    async fn create_table_version(
        &self,
        request: CreateTableVersionRequest,
    ) -> lance_namespace::Result<CreateTableVersionResponse> {
        self.ensure_request_table(request.id.as_ref())?;
        let version = u64::try_from(request.version).map_err(|_| {
            LanceNamespaceError::invalid_input("table version must be non-negative")
        })?;
        let naming_scheme = match request.naming_scheme.as_deref() {
            Some("V1") => ManifestNamingScheme::V1,
            _ => ManifestNamingScheme::V2,
        };
        let (object_store, base_path, _) = DatasetBuilder::from_uri(&self.table_uri())
            .build_object_store()
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        let staging_path = Path::from(request.manifest_path.clone());
        let manifest_data = object_store
            .inner
            .get(&staging_path)
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?
            .bytes()
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        let final_path = naming_scheme.manifest_path(&base_path, version);
        object_store
            .inner
            .put_opts(
                &final_path,
                manifest_data.into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| match e {
                ObjectStoreError::AlreadyExists { .. } | ObjectStoreError::Precondition { .. } => {
                    LanceNamespaceError::namespace_source(Box::new(
                        NamespaceError::ConcurrentModification {
                            message: format!(
                                "table version {} already exists for {}",
                                version,
                                self.table_key()
                            ),
                        },
                    ))
                }
                other => namespace_internal_error(other.to_string()),
            })?;
        let meta = object_store
            .inner
            .head(&final_path)
            .await
            .map_err(|e| namespace_internal_error(e.to_string()))?;
        match object_store.inner.delete(&staging_path).await {
            Ok(_) | Err(ObjectStoreError::NotFound { .. }) => {}
            Err(e) => return Err(namespace_internal_error(e.to_string())),
        }

        let mut response = CreateTableVersionResponse::new();
        response.version = Some(Box::new(TableVersion {
            version: version as i64,
            manifest_path: final_path.to_string(),
            manifest_size: Some(meta.size as i64),
            e_tag: meta.e_tag,
            timestamp_millis: None,
            metadata: request.metadata,
        }));
        Ok(response)
    }
}

pub(crate) async fn open_table_head_for_write(
    root_uri: &str,
    table_key: &str,
    table_path: &str,
    branch: Option<&str>,
) -> Result<Dataset> {
    load_table_from_namespace(
        staged_table_namespace(root_uri, table_key, table_path, branch),
        table_key,
        branch,
        None,
    )
    .await
}
