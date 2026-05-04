use std::env;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;
use object_store::{DynObjectStore, ObjectStore, PutPayload};
use url::Url;

use crate::error::{OmniError, Result};

const FILE_SCHEME_PREFIX: &str = "file://";
const S3_SCHEME_PREFIX: &str = "s3://";

#[async_trait]
pub trait StorageAdapter: Debug + Send + Sync {
    async fn read_text(&self, uri: &str) -> Result<String>;
    async fn write_text(&self, uri: &str, contents: &str) -> Result<()>;
    async fn exists(&self, uri: &str) -> Result<bool>;
    /// Move a file from `from_uri` to `to_uri`, replacing any existing file at
    /// `to_uri`. Atomic on local POSIX; on S3 implemented as copy + delete
    /// (NOT atomic — callers that depend on atomicity for crash recovery must
    /// tolerate "both source and destination exist after a crash").
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()>;
    /// Remove a file. Returns Ok(()) if the file does not exist.
    async fn delete(&self, uri: &str) -> Result<()>;
    /// List all files (non-recursively, files only) directly under `dir_uri`.
    /// Returns full URIs (same scheme as `dir_uri`). The result is unordered.
    /// Returns Ok(empty) if the directory does not exist or is empty.
    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Local,
    S3,
}

#[derive(Debug, Default)]
pub struct LocalStorageAdapter;

#[derive(Debug)]
pub struct S3StorageAdapter {
    bucket: String,
    store: Arc<DynObjectStore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Location {
    bucket: String,
    key: String,
}

#[async_trait]
impl StorageAdapter for LocalStorageAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        let path = local_path_from_uri(uri)?;
        Ok(tokio::fs::read_to_string(&path).await?)
    }

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        let path = local_path_from_uri(uri)?;
        // Ensure parent directory exists. S3 has no equivalent (PutObject
        // is path-agnostic). For local fs, callers like the recovery
        // sidecar protocol expect transparent directory creation under
        // the repo root (the `__recovery/` directory doesn't pre-exist;
        // first sidecar write creates it).
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }
        tokio::fs::write(&path, contents).await?;
        Ok(())
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        Ok(local_path_from_uri(uri)?.exists())
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        let from = local_path_from_uri(from_uri)?;
        let to = local_path_from_uri(to_uri)?;
        tokio::fs::rename(&from, &to).await?;
        Ok(())
    }

    async fn delete(&self, uri: &str) -> Result<()> {
        let path = local_path_from_uri(uri)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        let path = local_path_from_uri(dir_uri)?;
        let mut out = Vec::new();
        let mut entries = match tokio::fs::read_dir(&path).await {
            Ok(e) => e,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(err) => return Err(err.into()),
        };
        let dir_str = dir_uri.trim_end_matches('/');
        while let Some(entry) = entries.next_entry().await? {
            let ft = entry.file_type().await?;
            if !ft.is_file() {
                continue;
            }
            if let Some(name) = entry.file_name().to_str() {
                out.push(format!("{}/{}", dir_str, name));
            }
        }
        Ok(out)
    }
}

#[async_trait]
impl StorageAdapter for S3StorageAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        let location = self.object_path(uri)?;
        let bytes = self
            .store
            .get(&location)
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?
            .bytes()
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?;

        String::from_utf8(bytes.to_vec()).map_err(|err| {
            OmniError::manifest_internal(format!("storage read failed for '{}': {}", uri, err))
        })
    }

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        let location = self.object_path(uri)?;
        self.store
            .put(&location, PutPayload::from(contents.as_bytes().to_vec()))
            .await
            .map_err(|err| storage_backend_error("write", uri, err))?;
        Ok(())
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        let location = self.object_path(uri)?;
        match self.store.head(&location).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => {
                let mut entries = self.store.list(Some(&location));
                let has_prefix_entries = entries
                    .try_next()
                    .await
                    .map_err(|err| storage_backend_error("exists", uri, err))?
                    .is_some();
                Ok(has_prefix_entries)
            }
            Err(err) => Err(storage_backend_error("exists", uri, err)),
        }
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        // S3 has no atomic rename. Copy then delete; if the copy succeeds and
        // the delete fails (or the process crashes between them), both
        // source and destination exist with the same content. Recovery code
        // must tolerate this case — see schema_state::recover_schema_state_files.
        let from = self.object_path(from_uri)?;
        let to = self.object_path(to_uri)?;
        self.store
            .copy(&from, &to)
            .await
            .map_err(|err| storage_backend_error("rename:copy", from_uri, err))?;
        self.store
            .delete(&from)
            .await
            .map_err(|err| storage_backend_error("rename:delete", from_uri, err))?;
        Ok(())
    }

    async fn delete(&self, uri: &str) -> Result<()> {
        let location = self.object_path(uri)?;
        match self.store.delete(&location).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(err) => Err(storage_backend_error("delete", uri, err)),
        }
    }

    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        // Normalize: ensure the URI describes a directory (trailing '/') so
        // we don't match sibling paths with a shared prefix
        // (e.g. listing `__recovery` shouldn't match `__recovery_log/...`).
        let dir_with_slash = if dir_uri.ends_with('/') {
            dir_uri.to_string()
        } else {
            format!("{}/", dir_uri)
        };
        // object_store::Path strips the trailing '/'; re-add it for filtering.
        let prefix_loc = self.object_path(&dir_with_slash)?;
        let prefix_with_slash = format!("{}/", prefix_loc.as_ref());

        let mut entries = self.store.list(Some(&prefix_loc));
        let mut out = Vec::new();
        let bucket_root = format!("{}{}/", S3_SCHEME_PREFIX, self.bucket);
        while let Some(meta) = entries
            .try_next()
            .await
            .map_err(|err| storage_backend_error("list_dir", dir_uri, err))?
        {
            let key_str = meta.location.as_ref();
            // Require the directory boundary to filter out sibling-prefix
            // matches (object_store's `list` is prefix-based, not dir-based).
            if !key_str.starts_with(&prefix_with_slash) {
                continue;
            }
            let suffix = &key_str[prefix_with_slash.len()..];
            // Non-recursive: skip anything inside a sub-directory.
            if suffix.contains('/') {
                continue;
            }
            out.push(format!("{}{}", bucket_root, key_str));
        }
        Ok(out)
    }
}

impl S3StorageAdapter {
    fn from_root_uri(root_uri: &str) -> Result<Self> {
        let location = parse_s3_uri(root_uri)?;
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&location.bucket);

        if let Some(endpoint) = env::var("AWS_ENDPOINT_URL_S3")
            .ok()
            .or_else(|| env::var("AWS_ENDPOINT_URL").ok())
        {
            builder = builder.with_endpoint(&endpoint);
            if endpoint.starts_with("http://") || env_var_truthy("AWS_ALLOW_HTTP") {
                builder = builder.with_allow_http(true);
            }
        }

        if env_var_truthy("AWS_S3_FORCE_PATH_STYLE") {
            builder = builder.with_virtual_hosted_style_request(false);
        }

        let store = builder.build().map_err(|err| {
            OmniError::manifest_internal(format!(
                "failed to initialize s3 storage for '{}': {}",
                root_uri, err
            ))
        })?;

        Ok(Self {
            bucket: location.bucket,
            store: Arc::new(store),
        })
    }

    fn object_path(&self, uri: &str) -> Result<ObjectPath> {
        let location = parse_s3_uri(uri)?;
        if location.bucket != self.bucket {
            return Err(OmniError::manifest_internal(format!(
                "s3 storage bucket mismatch for '{}': expected '{}', found '{}'",
                uri, self.bucket, location.bucket
            )));
        }
        if location.key.is_empty() {
            return Err(OmniError::manifest_internal(format!(
                "s3 storage path is empty for '{}'",
                uri
            )));
        }
        ObjectPath::parse(&location.key).map_err(|err| {
            OmniError::manifest_internal(format!("invalid s3 object path for '{}': {}", uri, err))
        })
    }
}

pub fn storage_kind_for_uri(uri: &str) -> StorageKind {
    if uri.starts_with(S3_SCHEME_PREFIX) {
        StorageKind::S3
    } else {
        StorageKind::Local
    }
}

pub fn storage_for_uri(uri: &str) -> Result<Arc<dyn StorageAdapter>> {
    match storage_kind_for_uri(uri) {
        StorageKind::Local => Ok(Arc::new(LocalStorageAdapter)),
        StorageKind::S3 => Ok(Arc::new(S3StorageAdapter::from_root_uri(uri)?)),
    }
}

pub fn normalize_root_uri(uri: &str) -> Result<String> {
    match storage_kind_for_uri(uri) {
        StorageKind::Local => {
            let path = local_path_from_uri(uri)?;
            Ok(normalize_local_path(&path))
        }
        StorageKind::S3 => Ok(trim_trailing_slashes(uri)),
    }
}

pub fn join_uri(root_uri: &str, relative_path: &str) -> String {
    let relative_path = relative_path.trim_start_matches('/');
    match storage_kind_for_uri(root_uri) {
        StorageKind::S3 => {
            let root = trim_trailing_slashes(root_uri);
            if root.is_empty() {
                relative_path.to_string()
            } else {
                format!("{}/{}", root, relative_path)
            }
        }
        StorageKind::Local => {
            let root = if root_uri.starts_with(FILE_SCHEME_PREFIX) {
                local_path_from_file_uri(root_uri)
                    .map(|path| normalize_local_path(&path))
                    .unwrap_or_else(|_| trim_trailing_slashes(root_uri))
            } else {
                normalize_local_path(Path::new(root_uri))
            };
            let joined = Path::new(&root).join(relative_path);
            normalize_local_path(&joined)
        }
    }
}

fn local_path_from_uri(uri: &str) -> Result<PathBuf> {
    if uri.starts_with(FILE_SCHEME_PREFIX) {
        return local_path_from_file_uri(uri);
    }
    Ok(PathBuf::from(uri))
}

fn local_path_from_file_uri(uri: &str) -> Result<PathBuf> {
    let url = Url::parse(uri).map_err(|err| {
        OmniError::manifest_internal(format!("invalid file uri '{}': {}", uri, err))
    })?;
    url.to_file_path()
        .map_err(|_| OmniError::manifest_internal(format!("invalid file uri '{}'", uri)))
}

fn parse_s3_uri(uri: &str) -> Result<S3Location> {
    let url = Url::parse(uri).map_err(|err| {
        OmniError::manifest_internal(format!("invalid s3 uri '{}': {}", uri, err))
    })?;
    if url.scheme() != "s3" {
        return Err(OmniError::manifest_internal(format!(
            "unsupported s3 uri '{}'",
            uri
        )));
    }
    let bucket = url
        .host_str()
        .ok_or_else(|| OmniError::manifest_internal(format!("missing s3 bucket in '{}'", uri)))?;
    Ok(S3Location {
        bucket: bucket.to_string(),
        key: url.path().trim_start_matches('/').to_string(),
    })
}

fn storage_backend_error(action: &str, uri: &str, err: impl std::fmt::Display) -> OmniError {
    OmniError::manifest_internal(format!("storage {} failed for '{}': {}", action, uri, err))
}

fn normalize_local_path(path: &Path) -> String {
    let raw = path.as_os_str().to_string_lossy();
    if raw == "/" {
        return raw.to_string();
    }
    trim_trailing_slashes(&raw)
}

fn trim_trailing_slashes(value: &str) -> String {
    let trimmed = value.trim_end_matches('/');
    if trimmed.is_empty() {
        value.to_string()
    } else {
        trimmed.to_string()
    }
}

fn env_var_truthy(key: &str) -> bool {
    matches!(
        env::var(key).ok().as_deref(),
        Some("1" | "true" | "TRUE" | "True" | "yes" | "YES" | "on" | "ON")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_backend_selection_is_scheme_aware() {
        assert_eq!(storage_kind_for_uri("/tmp/repo"), StorageKind::Local);
        assert_eq!(storage_kind_for_uri("file:///tmp/repo"), StorageKind::Local);
        assert_eq!(
            storage_kind_for_uri("s3://omnigraph-preview/repo"),
            StorageKind::S3
        );
    }

    #[test]
    fn normalize_root_uri_preserves_local_and_s3_shapes() {
        assert_eq!(
            normalize_root_uri("/tmp/omnigraph/").unwrap(),
            "/tmp/omnigraph"
        );
        assert_eq!(
            normalize_root_uri("file:///tmp/omnigraph/").unwrap(),
            "/tmp/omnigraph"
        );
        assert_eq!(
            normalize_root_uri("s3://bucket/prefix/").unwrap(),
            "s3://bucket/prefix"
        );
    }

    #[test]
    fn join_uri_handles_local_file_and_s3_roots() {
        assert_eq!(
            join_uri("/tmp/omnigraph", "_schema.pg"),
            "/tmp/omnigraph/_schema.pg"
        );
        assert_eq!(
            join_uri("file:///tmp/omnigraph", "_schema.pg"),
            "/tmp/omnigraph/_schema.pg"
        );
        assert_eq!(
            join_uri("s3://bucket/prefix", "_schema.pg"),
            "s3://bucket/prefix/_schema.pg"
        );
    }

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        let location = parse_s3_uri("s3://bucket/repo/_schema.pg").unwrap();
        assert_eq!(location.bucket, "bucket");
        assert_eq!(location.key, "repo/_schema.pg");
    }
}
