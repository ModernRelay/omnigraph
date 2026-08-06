use std::sync::Arc;

use lance::Dataset;
use lance_namespace::Error as LanceNamespaceError;

use crate::error::{OmniError, Result};
use crate::storage::{StorageKind, join_uri, storage_kind_for_uri};

use super::TableIdentity;

const MANIFEST_DIR: &str = "__manifest";

pub(crate) fn manifest_uri(root: &str) -> String {
    format!("{}/{}", root.trim_end_matches('/'), MANIFEST_DIR)
}

#[cfg(test)]
pub(super) async fn open_manifest_dataset(root_uri: &str, branch: Option<&str>) -> Result<Dataset> {
    let control_session = crate::lance_access::control_session();
    open_manifest_dataset_with_session(root_uri, branch, &control_session).await
}

pub(super) async fn open_manifest_dataset_with_session(
    root_uri: &str,
    branch: Option<&str>,
    control_session: &Arc<lance::session::Session>,
) -> Result<Dataset> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset(
        &uri,
        crate::instrumentation::VersionResolution::Latest,
        Some(control_session),
        crate::instrumentation::manifest_wrapper(),
    )
    .await?;
    match branch {
        Some(branch) if branch != "main" => dataset
            .checkout_branch(branch)
            .await
            .map_err(|e| OmniError::Lance(e.to_string())),
        _ => Ok(dataset),
    }
}

fn format_table_version(version: u64) -> String {
    format!("{version:020}")
}

pub(super) fn table_object_id(identity: TableIdentity) -> String {
    format!(
        "table:{:016x}:{:016x}",
        identity.stable_table_id, identity.table_incarnation_id
    )
}

pub(super) fn version_object_id(identity: TableIdentity, version: u64) -> String {
    format!(
        "table_version:{:016x}:{:016x}:{}",
        identity.stable_table_id,
        identity.table_incarnation_id,
        format_table_version(version)
    )
}

pub(super) fn tombstone_object_id(identity: TableIdentity, version: u64) -> String {
    format!(
        "table_tombstone:{:016x}:{:016x}:{}",
        identity.stable_table_id,
        identity.table_incarnation_id,
        format_table_version(version)
    )
}

pub(super) fn table_id_to_key(request_id: Option<&Vec<String>>) -> lance_namespace::Result<String> {
    match request_id {
        Some(request_id) if request_id.len() == 1 && !request_id[0].is_empty() => {
            Ok(request_id[0].clone())
        }
        Some(request_id) => Err(LanceNamespaceError::invalid_input(format!(
            "expected single table id component, got {:?}",
            request_id
        ))),
        None => Err(LanceNamespaceError::invalid_input("table id is required")),
    }
}

pub(super) fn table_uri_for_path(root_uri: &str, table_path: &str, branch: Option<&str>) -> String {
    let mut dataset_location = join_uri(root_uri, table_path);
    if let Some(branch) = branch.filter(|branch| *branch != "main") {
        dataset_location = join_uri(&dataset_location, "tree");
        for segment in branch.split('/') {
            dataset_location = join_uri(&dataset_location, segment);
        }
    }
    match storage_kind_for_uri(root_uri) {
        StorageKind::Local => url::Url::from_file_path(&dataset_location)
            .map(|uri| uri.to_string())
            .unwrap_or(dataset_location),
        StorageKind::S3 => dataset_location,
    }
}

#[cfg(test)]
pub(super) fn namespace_internal_error(message: impl Into<String>) -> LanceNamespaceError {
    LanceNamespaceError::namespace_source(Box::new(std::io::Error::other(message.into())))
}
