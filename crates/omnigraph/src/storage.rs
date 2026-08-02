//! Compatibility facade over the shared storage implementation.
//!
//! `omnigraph-storage` owns the only local/S3 implementation so lower
//! control-plane crates do not depend back on the engine. This module keeps
//! the v10 engine API intact: the public trait and constructors still return
//! [`crate::error::Result`] / [`crate::error::OmniError`].

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use crate::error::Result;

pub use omnigraph_storage::{StorageKind, join_uri};

#[async_trait]
pub trait StorageAdapter: Debug + Send + Sync {
    async fn read_text(&self, uri: &str) -> Result<String>;
    /// Read a text object if it exists using one backend GET.
    ///
    /// Returns `Ok(None)` only when the object store reports `NotFound`.
    /// Every other transport, permission, body-read, or UTF-8 failure remains
    /// a loud error. Callers must use this instead of `exists()` followed by
    /// `read_text()` when disappearance between the probe and read is a valid
    /// concurrent outcome.
    async fn read_text_if_exists(&self, uri: &str) -> Result<Option<String>>;
    /// Read at most `max_bytes + 1` bytes and reject an oversized object
    /// before its full body can be materialized.
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<String>>;
    async fn write_text(&self, uri: &str, contents: &str) -> Result<()>;
    /// Write a text object only if no object exists at `uri`.
    ///
    /// Returns `Ok(true)` when this call created the object, `Ok(false)`
    /// when the object already existed, and propagates every other storage
    /// error. Callers use this to establish ownership before running
    /// best-effort cleanup on partial failure.
    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool>;
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
    /// Consumers must tolerate non-payload residue appearing in storage
    /// (backend staging files are filtered by the backend, but crash residue
    /// of any future producer may not be) — filter by suffix, never assume
    /// every entry is yours.
    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>>;
    /// Read a text object together with its backend version token (stores
    /// with conditional-update support: the object's ETag; local: sha256 of
    /// the content). The token is opaque — valid only for
    /// `write_text_if_match` against the same adapter.
    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)>;
    /// Replace the object at `uri` only if its current version still matches
    /// `expected_version` (obtained from a prior versioned read/write on this
    /// adapter). Returns `Ok(Some(new_version))` on success and `Ok(None)`
    /// when the precondition failed (a concurrent writer won — the CAS-lost
    /// case callers must surface, never swallow). Stores with conditional
    /// updates (S3, in-memory) use a true conditional put (If-Match); the
    /// local filesystem has no such primitive (`PutMode::Update` is
    /// unimplemented upstream), so local compares content then replaces via
    /// an atomic staged write — the same single-machine semantics the
    /// callers had before this trait, safe under the callers' own lock
    /// protocol but not a cross-process barrier by itself (see the Known
    /// Gaps entry in docs/dev/invariants.md).
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>>;
    /// Recursively delete every object under `prefix_uri`. Returns Ok(())
    /// when nothing exists there (idempotent). Local: `remove_dir_all`
    /// (directories are a local-FS concept; list+delete would leave empty
    /// directory skeletons that local existence probes report as present);
    /// object stores: list + delete (NOT atomic — callers must tolerate
    /// partial prefixes on crash, which the cluster delete protocol does by
    /// retry).
    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()>;
}

/// Engine-facing wrapper over the one lower storage implementation.
#[derive(Debug)]
pub struct ObjectStorageAdapter {
    inner: omnigraph_storage::ObjectStorageAdapter,
}

impl ObjectStorageAdapter {
    /// Local-filesystem backend rooted at `/`. URIs are plain paths or
    /// `file://` URIs; relative paths are lexically absolutized against the
    /// current working directory.
    pub fn local() -> Self {
        Self {
            inner: omnigraph_storage::ObjectStorageAdapter::local(),
        }
    }

    /// S3 backend scoped to the bucket named in `root_uri`. Credentials and
    /// endpoint come from the standard `AWS_*` environment variables (the
    /// same ones Lance reads for its dataset stores).
    pub fn s3_from_root_uri(root_uri: &str) -> Result<Self> {
        Ok(Self {
            inner: omnigraph_storage::ObjectStorageAdapter::s3_from_root_uri(root_uri)?,
        })
    }

    /// In-memory backend for tests and embedded experiments. Implements the
    /// full contract including true conditional updates.
    pub fn in_memory() -> Self {
        Self {
            inner: omnigraph_storage::ObjectStorageAdapter::in_memory(),
        }
    }
}

#[async_trait]
impl StorageAdapter for ObjectStorageAdapter {
    async fn read_text(&self, uri: &str) -> Result<String> {
        Ok(omnigraph_storage::StorageAdapter::read_text(&self.inner, uri).await?)
    }

    async fn read_text_if_exists(&self, uri: &str) -> Result<Option<String>> {
        Ok(omnigraph_storage::StorageAdapter::read_text_if_exists(&self.inner, uri).await?)
    }

    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> Result<Option<String>> {
        Ok(
            omnigraph_storage::StorageAdapter::read_text_if_exists_bounded(
                &self.inner,
                uri,
                max_bytes,
            )
            .await?,
        )
    }

    async fn write_text(&self, uri: &str, contents: &str) -> Result<()> {
        Ok(omnigraph_storage::StorageAdapter::write_text(&self.inner, uri, contents).await?)
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
        Ok(
            omnigraph_storage::StorageAdapter::write_text_if_absent(&self.inner, uri, contents)
                .await?,
        )
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        Ok(omnigraph_storage::StorageAdapter::exists(&self.inner, uri).await?)
    }

    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> Result<()> {
        Ok(omnigraph_storage::StorageAdapter::rename_text(&self.inner, from_uri, to_uri).await?)
    }

    async fn delete(&self, uri: &str) -> Result<()> {
        Ok(omnigraph_storage::StorageAdapter::delete(&self.inner, uri).await?)
    }

    async fn list_dir(&self, dir_uri: &str) -> Result<Vec<String>> {
        Ok(omnigraph_storage::StorageAdapter::list_dir(&self.inner, dir_uri).await?)
    }

    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
        Ok(omnigraph_storage::StorageAdapter::read_text_versioned(&self.inner, uri).await?)
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>> {
        Ok(omnigraph_storage::StorageAdapter::write_text_if_match(
            &self.inner,
            uri,
            contents,
            expected_version,
        )
        .await?)
    }

    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
        Ok(omnigraph_storage::StorageAdapter::delete_prefix(&self.inner, prefix_uri).await?)
    }
}

pub fn storage_kind_for_uri(uri: &str) -> StorageKind {
    omnigraph_storage::storage_kind_for_uri(uri)
}

pub fn storage_for_uri(uri: &str) -> Result<Arc<dyn StorageAdapter>> {
    match storage_kind_for_uri(uri) {
        StorageKind::Local => Ok(Arc::new(ObjectStorageAdapter::local())),
        StorageKind::S3 => Ok(Arc::new(ObjectStorageAdapter::s3_from_root_uri(uri)?)),
    }
}

pub fn normalize_root_uri(uri: &str) -> Result<String> {
    Ok(omnigraph_storage::normalize_root_uri(uri)?)
}

pub(crate) fn write_queue_root_identity(normalized_root: &str) -> Result<String> {
    Ok(omnigraph_storage::write_queue_root_identity(
        normalized_root,
    )?)
}
