use std::collections::HashMap;

use lance::Dataset;
use lance_namespace::Error as LanceNamespaceError;
use lance_namespace::models::CreateTableVersionRequest;
#[cfg(test)]
use lance_namespace::models::TableVersion;
use serde::{Deserialize, Serialize};

use crate::error::{OmniError, Result};
use crate::storage::{StorageKind, join_uri, storage_kind_for_uri};

use super::layout::table_id_to_key;

pub(super) const OMNIGRAPH_ROW_COUNT_KEY: &str = "omnigraph.row_count";
const OMNIGRAPH_TABLE_BRANCH_KEY: &str = "omnigraph.table_branch";
const OMNIGRAPH_TABLE_FORK_OWNER_KEY: &str = "omnigraph.table_fork_owner";

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
        table_fork_owner: metadata.get(OMNIGRAPH_TABLE_FORK_OWNER_KEY).cloned(),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    table_fork_owner: Option<String>,
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
                dataset.manifest_location().path.as_ref(),
            )?,
            manifest_size: dataset.manifest_location().size,
            e_tag: dataset.manifest_location().e_tag.clone(),
            naming_scheme: Some(format!("{:?}", dataset.manifest_location().naming_scheme)),
            table_fork_owner: None,
        })
    }

    pub(crate) fn table_fork_owner(&self) -> Option<&str> {
        self.table_fork_owner.as_deref()
    }

    pub(crate) fn with_table_fork_owner(mut self, owner: Option<&str>) -> Self {
        self.table_fork_owner = owner.map(str::to_string);
        self
    }

    pub(crate) fn is_table_fork_of(&self, fork: &str, owner: &str) -> bool {
        self.table_fork_owner
            .as_deref()
            .map_or(fork == owner, |recorded| recorded == owner)
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
        let mut metadata = namespace_version_metadata(row_count, table_branch);
        if let Some(owner) = &self.table_fork_owner {
            metadata.insert(OMNIGRAPH_TABLE_FORK_OWNER_KEY.to_string(), owner.clone());
        }
        request.metadata = Some(metadata);
        request
    }

    #[cfg(test)]
    pub(super) fn to_namespace_version(&self, version: u64) -> TableVersion {
        self.to_namespace_version_with_details(version, None, None)
    }

    #[cfg(test)]
    pub(super) fn to_namespace_version_with_details(
        &self,
        version: u64,
        timestamp_millis: Option<i64>,
        metadata: Option<HashMap<String, String>>,
    ) -> TableVersion {
        let mut metadata = metadata.unwrap_or_default();
        if let Some(owner) = &self.table_fork_owner {
            metadata.insert(OMNIGRAPH_TABLE_FORK_OWNER_KEY.to_string(), owner.clone());
        }
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

pub(super) fn object_store_path_from_uri(uri: &str) -> Result<String> {
    match storage_kind_for_uri(uri)? {
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
                OmniError::manifest_internal(format!(
                    "invalid remote object-store uri '{}': {}",
                    uri, e
                ))
            })?;
            Ok(url.path().trim_start_matches('/').to_string())
        }
        StorageKind::Azure => {
            let url = url::Url::parse(uri).map_err(|e| {
                OmniError::manifest_internal(format!(
                    "invalid remote object-store uri '{}': {}",
                    uri, e
                ))
            })?;
            object_store::path::Path::from_url_path(url.path())
                .map(|path| path.to_string())
                .map_err(|e| {
                    OmniError::manifest_internal(format!(
                        "invalid remote object-store path in '{}': {}",
                        uri, e
                    ))
                })
        }
    }
}

fn full_manifest_object_store_path(
    root_uri: &str,
    table_path: &str,
    manifest_path: &str,
) -> Result<String> {
    // Lance may spell the same local object-store root through different
    // filesystem aliases across a commit handle and a later reopen (notably
    // `/var` versus `/private/var` on macOS). Once the canonical graph-relative
    // table path is present, discard that unstable raw prefix and rebuild the
    // object path from the graph root plus the suffix below the dataset.
    if let Some((_, suffix)) = manifest_path.rsplit_once(table_path) {
        let dataset_uri = join_uri(root_uri, table_path);
        let dataset_path = object_store_path_from_uri(&dataset_uri)?;
        let suffix = suffix.trim_start_matches('/');
        return if suffix.is_empty() {
            Ok(dataset_path)
        } else {
            Ok(format!("{}/{}", dataset_path.trim_end_matches('/'), suffix))
        };
    }

    if manifest_path.contains("://") {
        return object_store_path_from_uri(manifest_path);
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
    let ds = crate::instrumentation::open_dataset(
        &full_path,
        crate::instrumentation::VersionResolution::Latest,
        None,
        crate::instrumentation::table_wrapper(),
    )
    .await?;
    let ds = match branch {
        Some(branch) => ds
            .checkout_branch(branch)
            .await
            .map_err(OmniError::storage)?,
        None => ds,
    };
    let ds = ds
        .checkout_version(version)
        .await
        .map_err(OmniError::storage)?;
    TableVersionMetadata::from_dataset(root_uri, table_path, &ds)
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEGACY_JSON: &str = r#"{
        "manifest_path":"graph/nodes/person/_versions/7.manifest",
        "manifest_size":321,
        "e_tag":"version-etag",
        "naming_scheme":"V2"
    }"#;
    const OWNER: &str = "source.01ARZ3NDEKTSV4RRFFQ69G5FAV";
    const TARGET: &str = "target.01ARZ3NDEKTSV4RRFFQ69G5FAW";
    const FORK: &str = "fork.01ARZ3NDEKTSV4RRFFQ69G5FAV.m42.01ARZ3NDEKTSV4RRFFQ69G5FAX";

    /// GQT cannot inject absent owner metadata or inspect its serialized omission.
    #[test]
    fn legacy_metadata_owns_only_the_exact_native_ref() {
        let metadata = TableVersionMetadata::from_json_str(LEGACY_JSON).unwrap();
        assert_eq!(metadata.table_fork_owner(), None);
        for legacy_ref in ["foo", OWNER, "foo.m42.01ARZ3NDEKTSV4RRFFQ69G5FAV"] {
            assert!(metadata.is_table_fork_of(legacy_ref, legacy_ref));
            assert!(!metadata.is_table_fork_of(legacy_ref, TARGET));
        }
        assert!(!metadata.is_table_fork_of("foo.m42.01ARZ3NDEKTSV4RRFFQ69G5FAV", "foo"));
        assert!(!metadata.is_table_fork_of(FORK, OWNER));
        let encoded = metadata.to_json_string().unwrap();
        let value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert!(value.get("table_fork_owner").is_none());
        assert_eq!(
            TableVersionMetadata::from_json_str(&encoded).unwrap(),
            metadata
        );
    }

    /// GQT cannot forge legacy refs and metadata markers independently.
    #[test]
    fn explicit_owner_distinguishes_a_new_fork_from_a_legacy_lookalike() {
        let legacy = TableVersionMetadata::from_json_str(LEGACY_JSON).unwrap();
        let marked = legacy.clone().with_table_fork_owner(Some("foo"));
        let lookalike = "foo.m42.01ARZ3NDEKTSV4RRFFQ69G5FAV";
        assert!(!legacy.is_table_fork_of(lookalike, "foo"));
        assert!(marked.is_table_fork_of(lookalike, "foo"));
        assert!(!marked.is_table_fork_of(lookalike, lookalike));
        assert!(!marked.is_table_fork_of(lookalike, TARGET));
    }

    /// GQT does not expose physical version metadata serialization.
    #[test]
    fn owner_metadata_json_roundtrip_preserves_physical_version_fields() {
        let metadata = TableVersionMetadata::from_json_str(LEGACY_JSON)
            .unwrap()
            .with_table_fork_owner(Some(OWNER));
        let encoded = metadata.to_json_string().unwrap();
        let decoded = TableVersionMetadata::from_json_str(&encoded).unwrap();
        assert_eq!(decoded, metadata);
        assert_eq!(decoded.table_fork_owner(), Some(OWNER));
        assert!(decoded.is_table_fork_of(FORK, OWNER));
        assert!(!decoded.is_table_fork_of(FORK, TARGET));

        let main = decoded.with_table_fork_owner(None);
        assert_eq!(
            main,
            TableVersionMetadata::from_json_str(LEGACY_JSON).unwrap()
        );
    }

    /// GQT does not expose the namespace registration request and response metadata.
    #[test]
    fn namespace_pointer_roundtrip_keeps_source_ownership() {
        let source = TableVersionMetadata::from_json_str(LEGACY_JSON)
            .unwrap()
            .with_table_fork_owner(Some(OWNER));
        let request = source.to_create_table_version_request("node:Person", 7, 3, Some(FORK));
        assert_eq!(
            request
                .metadata
                .as_ref()
                .unwrap()
                .get(OMNIGRAPH_TABLE_FORK_OWNER_KEY)
                .map(String::as_str),
            Some(OWNER)
        );
        let (table_key, version, rows, native_ref, target_registration) =
            parse_namespace_version_request(&request).unwrap();
        assert_eq!(table_key, "node:Person");
        assert_eq!(version, 7);
        assert_eq!(rows, 3);
        assert_eq!(native_ref.as_deref(), Some(FORK));
        assert_eq!(target_registration, source);
        assert!(target_registration.is_table_fork_of(FORK, OWNER));
        assert!(!target_registration.is_table_fork_of(FORK, TARGET));

        let response = target_registration.to_namespace_version_with_details(
            7,
            Some(123),
            Some(HashMap::from([(
                "producer".to_string(),
                "test".to_string(),
            )])),
        );
        let metadata = response.metadata.unwrap();
        assert_eq!(
            metadata
                .get(OMNIGRAPH_TABLE_FORK_OWNER_KEY)
                .map(String::as_str),
            Some(OWNER)
        );
        assert_eq!(metadata.get("producer").map(String::as_str), Some("test"));
    }

    /// GQT cannot inject malformed persisted owner JSON types.
    #[test]
    fn malformed_owner_json_types_are_rejected() {
        for owner in [
            serde_json::json!(42),
            serde_json::json!({"name": OWNER}),
            serde_json::json!([OWNER]),
        ] {
            let mut value: serde_json::Value = serde_json::from_str(LEGACY_JSON).unwrap();
            value["table_fork_owner"] = owner;
            assert!(TableVersionMetadata::from_json_str(&value.to_string()).is_err());
        }
    }
}
