use std::collections::HashMap;

use lance::Dataset;
use lance_namespace::Error as LanceNamespaceError;
use lance_namespace::models::{CreateTableVersionRequest, TableVersion};
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};
use crate::storage::{StorageKind, join_uri, storage_kind_for_uri};

use super::layout::table_id_to_key;

pub(super) const OMNIGRAPH_ROW_COUNT_KEY: &str = "omnigraph.row_count";
const OMNIGRAPH_TABLE_BRANCH_KEY: &str = "omnigraph.table_branch";

pub(super) fn namespace_version_metadata(
    row_count: u64,
    table_branch: Option<&str>,
) -> HashMap<String, String> {
    let mut metadata =
        HashMap::from([(OMNIGRAPH_ROW_COUNT_KEY.to_string(), row_count.to_string())]);
    if let Some(table_branch) = table_branch {
        metadata.insert(
            OMNIGRAPH_TABLE_BRANCH_KEY.to_string(),
            table_branch.to_string(),
        );
    }
    metadata
}

pub(super) fn parse_namespace_version_request(
    request: &CreateTableVersionRequest,
) -> lance_namespace::Result<(String, u64, u64, Option<String>, TableVersionMetadata)> {
    let table_key = table_id_to_key(request.id.as_ref())?;
    let version = u64::try_from(request.version)
        .map_err(|_| LanceNamespaceError::invalid_input("table version must be non-negative"))?;
    let metadata = request.metadata.as_ref().ok_or_else(|| {
        LanceNamespaceError::invalid_input("version metadata is required for Omnigraph rows")
    })?;
    let row_count = metadata
        .get(OMNIGRAPH_ROW_COUNT_KEY)
        .ok_or_else(|| {
            LanceNamespaceError::invalid_input("missing omnigraph.row_count in metadata")
        })?
        .parse::<u64>()
        .map_err(|e| {
            LanceNamespaceError::invalid_input(format!("invalid omnigraph.row_count value: {}", e))
        })?;
    let table_branch = metadata.get(OMNIGRAPH_TABLE_BRANCH_KEY).cloned();
    let version_metadata = TableVersionMetadata {
        manifest_path: request.manifest_path.clone(),
        manifest_size: request.manifest_size.map(|size| size as u64),
        e_tag: request.e_tag.clone(),
        naming_scheme: request.naming_scheme.clone(),
    };

    Ok((
        table_key,
        version,
        row_count,
        table_branch,
        version_metadata,
    ))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TableVersionMetadata {
    manifest_path: String,
    manifest_size: Option<u64>,
    e_tag: Option<String>,
    naming_scheme: Option<String>,
}

impl TableVersionMetadata {
    pub(crate) fn from_dataset(
        root_uri: &str,
        table_path: &str,
        dataset: &Dataset,
    ) -> Result<Self> {
        Ok(Self {
            manifest_path: full_manifest_object_store_path(
                root_uri,
                table_path,
                &dataset.manifest_location().path.to_string(),
            )?,
            manifest_size: dataset.manifest_location().size,
            e_tag: dataset.manifest_location().e_tag.clone(),
            naming_scheme: Some(format!("{:?}", dataset.manifest_location().naming_scheme)),
        })
    }

    pub(super) fn from_json_str(value: &str) -> Result<Self> {
        serde_json::from_str(value).map_err(|e| {
            OmniError::manifest_internal(format!("failed to decode manifest metadata: {e}"))
        })
    }

    pub(super) fn to_json_string(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| {
            OmniError::manifest_internal(format!("failed to encode manifest metadata: {e}"))
        })
    }

    #[cfg(test)]
    pub(crate) fn manifest_path(&self) -> &str {
        &self.manifest_path
    }

    #[cfg(test)]
    pub(crate) fn manifest_size(&self) -> Option<u64> {
        self.manifest_size
    }

    #[cfg(test)]
    pub(crate) fn e_tag(&self) -> Option<&str> {
        self.e_tag.as_deref()
    }

    #[cfg(test)]
    pub(crate) fn naming_scheme(&self) -> Option<&str> {
        self.naming_scheme.as_deref()
    }

    pub(crate) fn to_create_table_version_request(
        &self,
        table_key: &str,
        table_version: u64,
        row_count: u64,
        table_branch: Option<&str>,
    ) -> CreateTableVersionRequest {
        let mut request =
            CreateTableVersionRequest::new(table_version as i64, self.manifest_path.clone());
        request.id = Some(vec![table_key.to_string()]);
        request.manifest_size = self.manifest_size.map(|size| size as i64);
        request.e_tag = self.e_tag.clone();
        request.naming_scheme = self.naming_scheme.clone();
        request.metadata = Some(namespace_version_metadata(row_count, table_branch));
        request
    }

    pub(super) fn to_namespace_version(&self, version: u64) -> TableVersion {
        self.to_namespace_version_with_details(version, None, None)
    }

    pub(super) fn to_namespace_version_with_details(
        &self,
        version: u64,
        timestamp_millis: Option<i64>,
        metadata: Option<HashMap<String, String>>,
    ) -> TableVersion {
        let mut metadata = metadata.unwrap_or_default();
        if let Some(naming_scheme) = &self.naming_scheme {
            metadata.insert("naming_scheme".to_string(), naming_scheme.clone());
        }

        TableVersion {
            version: version as i64,
            manifest_path: self.manifest_path.clone(),
            manifest_size: self.manifest_size.map(|size| size as i64),
            e_tag: self.e_tag.clone(),
            timestamp_millis,
            metadata: (!metadata.is_empty()).then_some(metadata),
        }
    }
}

fn object_store_path_from_uri(uri: &str) -> Result<String> {
    match storage_kind_for_uri(uri) {
        StorageKind::Local => {
            if uri.strip_prefix("file://").is_some() {
                let path = url::Url::parse(uri)
                    .map_err(|e| {
                        OmniError::manifest_internal(format!("invalid file uri '{}': {}", uri, e))
                    })?
                    .to_file_path()
                    .map_err(|_| {
                        OmniError::manifest_internal(format!("invalid file uri '{}'", uri))
                    })?;
                Ok(path.to_string_lossy().to_string())
            } else {
                Ok(uri.to_string())
            }
        }
        StorageKind::S3 => {
            let url = url::Url::parse(uri).map_err(|e| {
                OmniError::manifest_internal(format!("invalid s3 uri '{}': {}", uri, e))
            })?;
            Ok(url.path().trim_start_matches('/').to_string())
        }
    }
}

fn full_manifest_object_store_path(
    root_uri: &str,
    table_path: &str,
    manifest_path: &str,
) -> Result<String> {
    if manifest_path.contains("://") {
        return object_store_path_from_uri(manifest_path);
    }

    if manifest_path.contains(table_path) {
        return Ok(manifest_path.to_string());
    }

    let dataset_uri = join_uri(root_uri, table_path);
    let dataset_path = object_store_path_from_uri(&dataset_uri)?;
    let manifest_path = manifest_path.trim_start_matches('/');

    if manifest_path.is_empty() {
        return Ok(dataset_path);
    }

    Ok(format!(
        "{}/{}",
        dataset_path.trim_end_matches('/'),
        manifest_path
    ))
}

#[cfg(test)]
pub(super) async fn table_version_metadata_for_state(
    root_uri: &str,
    table_path: &str,
    branch: Option<&str>,
    version: u64,
) -> Result<TableVersionMetadata> {
    let full_path = format!("{}/{}", root_uri.trim_end_matches('/'), table_path);
    let ds = Dataset::open(&full_path)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    let ds = match branch {
        Some(branch) => ds
            .checkout_branch(branch)
            .await
            .map_err(|e| OmniError::Lance(e.to_string()))?,
        None => ds,
    };
    let ds = ds
        .checkout_version(version)
        .await
        .map_err(|e| OmniError::Lance(e.to_string()))?;
    TableVersionMetadata::from_dataset(root_uri, table_path, &ds)
}
