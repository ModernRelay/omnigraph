use lance::Dataset;
use lance_namespace::Error as LanceNamespaceError;

use crate::error::{OmniError, Result};
use crate::storage::{StorageKind, join_uri, storage_kind_for_uri};

const MANIFEST_DIR: &str = "__manifest";

pub(super) fn type_name_hash(name: &str) -> String {
    let mut h: u64 = 0xcbf29ce484222325;
    for byte in name.as_bytes() {
        h ^= *byte as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", h)
}

pub(crate) fn manifest_uri(root: &str) -> String {
    format!("{}/{}", root.trim_end_matches('/'), MANIFEST_DIR)
}

pub(super) async fn open_manifest_dataset(root_uri: &str, branch: Option<&str>) -> Result<Dataset> {
    let uri = manifest_uri(root_uri.trim_end_matches('/'));
    let dataset = crate::instrumentation::open_dataset_tracked(
        &uri,
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

pub(super) fn version_object_id(table_key: &str, version: u64) -> String {
    format!("{}${}", table_key, format_table_version(version))
}

pub(super) fn tombstone_object_id(table_key: &str, version: u64) -> String {
    format!("{}$tombstone${}", table_key, format_table_version(version))
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
    table_uri_for_location(&join_uri(root_uri, table_path), branch)
}

/// Branch-encode + scheme-normalize a table's BASE location (the `root/table_path`
/// join) into the URI Lance opens. Factored from `table_uri_for_path` so a caller
/// that already holds the joined `full_path` (the write staging path) builds the
/// identical pinned-open location without re-deriving `table_path`. For a non-main
/// branch this appends `tree/{branch}` so the branch's own version chain resolves
/// without a `checkout_branch`; local locations are converted to `file://`.
pub(crate) fn table_uri_for_location(location: &str, branch: Option<&str>) -> String {
    let mut dataset_location = location.to_string();
    if let Some(branch) = branch.filter(|branch| *branch != "main") {
        dataset_location = join_uri(&dataset_location, "tree");
        for segment in branch.split('/') {
            dataset_location = join_uri(&dataset_location, segment);
        }
    }
    match storage_kind_for_uri(location) {
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
