use std::env;
use std::fmt::Debug;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{DynObjectStore, ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use url::Url;

use crate::error::{OmniError, Result};

const FILE_SCHEME_PREFIX: &str = "file://";
const S3_SCHEME_PREFIX: &str = "s3://";

#[async_trait]
pub trait StorageAdapter: Debug + Send + Sync {
    async fn read_text(&self, uri: &str) -> Result<String>;
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

/// Version token for local files: content identity. The local filesystem
/// backend reports mtime-derived ETags too coarse for CAS (sub-granularity
/// rewrites collide); sha256 is stable, cheap at these object sizes, and
/// already the cluster ledger's CAS vocabulary.
fn local_version_token(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Local,
    S3,
}

/// The one storage implementation: every backend is an
/// [`object_store::ObjectStore`], so the semantics (atomic-visibility puts,
/// conditional creates, path-delimited listing) are upstream-maintained and
/// identical across backends by construction. The per-backend residue is
/// confined to [`UriCodec`] (URI ↔ object path mapping) and the
/// `supports_conditional_update` capability flag (false only for the local
/// filesystem, where upstream `PutMode::Update` is unimplemented).
#[derive(Debug)]
pub struct ObjectStorageAdapter {
    store: Arc<DynObjectStore>,
    codec: UriCodec,
    /// Whether the backend implements `PutMode::Update` (ETag-conditioned
    /// put). Gates BOTH the version-token source in `read_text_versioned`
    /// and the `write_text_if_match` strategy — the two must agree or every
    /// CAS loses.
    supports_conditional_update: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum UriCodec {
    /// Plain absolute/relative paths or `file://` URIs, mapped onto a
    /// root-anchored [`LocalFileSystem`].
    Local,
    /// `s3://{bucket}/{key}` URIs, mapped onto a bucket-scoped store.
    S3 { bucket: String },
    /// Opaque keys for the in-memory test/embedded backend; leading
    /// slashes are stripped.
    Memory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Location {
    bucket: String,
    key: String,
}

impl ObjectStorageAdapter {
    /// Local-filesystem backend rooted at `/`. URIs are plain paths or
    /// `file://` URIs; relative paths are lexically absolutized against the
    /// current working directory.
    pub fn local() -> Self {
        Self {
            store: Arc::new(LocalFileSystem::new()),
            codec: UriCodec::Local,
            supports_conditional_update: false,
        }
    }

    /// S3 backend scoped to the bucket named in `root_uri`. Credentials and
    /// endpoint come from the standard `AWS_*` environment variables (the
    /// same ones Lance reads for its dataset stores).
    pub fn s3_from_root_uri(root_uri: &str) -> Result<Self> {
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
            store: Arc::new(store),
            codec: UriCodec::S3 {
                bucket: location.bucket,
            },
            supports_conditional_update: true,
        })
    }

    /// In-memory backend for tests and embedded experiments. Implements the
    /// FULL contract including true conditional updates (unlike the local
    /// filesystem), so contract tests exercise the strong-CAS path without a
    /// bucket. State lives only as long as the adapter.
    pub fn in_memory() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
            codec: UriCodec::Memory,
            supports_conditional_update: true,
        }
    }

    fn object_path(&self, uri: &str) -> Result<ObjectPath> {
        match &self.codec {
            UriCodec::Local => {
                let path = absolutize_lexically(local_path_from_uri(uri)?)?;
                ObjectPath::from_absolute_path(&path).map_err(|err| {
                    OmniError::manifest_internal(format!(
                        "invalid local object path for '{}': {}",
                        uri, err
                    ))
                })
            }
            UriCodec::S3 { bucket } => {
                let location = parse_s3_uri(uri)?;
                if &location.bucket != bucket {
                    return Err(OmniError::manifest_internal(format!(
                        "s3 storage bucket mismatch for '{}': expected '{}', found '{}'",
                        uri, bucket, location.bucket
                    )));
                }
                if location.key.is_empty() {
                    return Err(OmniError::manifest_internal(format!(
                        "s3 storage path is empty for '{}'",
                        uri
                    )));
                }
                ObjectPath::parse(&location.key).map_err(|err| {
                    OmniError::manifest_internal(format!(
                        "invalid s3 object path for '{}': {}",
                        uri, err
                    ))
                })
            }
            UriCodec::Memory => {
                ObjectPath::parse(uri.trim_start_matches('/')).map_err(|err| {
                    OmniError::manifest_internal(format!(
                        "invalid memory object path for '{}': {}",
                        uri, err
                    ))
                })
            }
        }
    }
}

#[async_trait]
impl StorageAdapter for ObjectStorageAdapter {
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
        // Atomic visibility is the backend's contract: object stores via
        // PutObject; LocalFileSystem via an internal staged-temp + rename
        // (a reader sees the old object or the new one, never a truncated
        // in-progress write). Callers (sidecar protocol, cluster state)
        // assume it.
        let location = self.object_path(uri)?;
        self.store
            .put(&location, PutPayload::from(contents.as_bytes().to_vec()))
            .await
            .map_err(|err| storage_backend_error("write", uri, err))?;
        Ok(())
    }

    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> Result<bool> {
        // PutMode::Create: atomic no-replace publish on every backend —
        // exactly one of N concurrent claimants wins, and the winner's
        // object is fully readable at the instant it becomes visible
        // (LocalFileSystem stages the temp file completely, then
        // hard_links it; pinned by
        // `local_write_text_if_absent_is_read_visible_on_return`).
        let location = self.object_path(uri)?;
        match self
            .store
            .put_opts(
                &location,
                PutPayload::from(contents.as_bytes().to_vec()),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => Ok(false),
            Err(err) => Err(storage_backend_error("write_if_absent", uri, err)),
        }
    }

    async fn exists(&self, uri: &str) -> Result<bool> {
        // head() answers for objects; the list fallback answers for
        // "directory-shaped" URIs (e.g. a Lance dataset root, whose
        // `_versions/*.manifest` makes any committed dataset non-empty).
        // Object-store semantics throughout: only objects exist —
        // an EMPTY local directory does not (callers that probe local
        // directories use std::fs directly).
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
        // ObjectStore::rename: LocalFileSystem overrides it with an atomic
        // fs::rename (creating missing destination parents); object stores
        // use the default copy + delete — if the copy succeeds and the
        // delete fails (or the process crashes between them), both source
        // and destination exist with the same content. Recovery code must
        // tolerate this case — see schema_state::recover_schema_state_files.
        let from = self.object_path(from_uri)?;
        let to = self.object_path(to_uri)?;
        self.store
            .rename(&from, &to)
            .await
            .map_err(|err| storage_backend_error("rename", from_uri, err))?;
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
        // list_with_delimiter is non-recursive and path-delimited on every
        // backend (no sibling-prefix bleed: listing `__recovery` cannot
        // match `__recovery_log/...`), and returns Ok(empty) for a missing
        // directory. Output URIs are anchored on the INPUT `dir_uri` plus
        // the entry filename, so the strings round-trip byte-identically
        // into read_text/delete regardless of scheme (plain path, file://,
        // s3://).
        let anchor = dir_uri.trim_end_matches('/');
        let prefix = self.object_path(anchor)?;
        let listing = self
            .store
            .list_with_delimiter(Some(&prefix))
            .await
            .map_err(|err| storage_backend_error("list_dir", dir_uri, err))?;
        let mut out = Vec::with_capacity(listing.objects.len());
        for meta in listing.objects {
            if let Some(name) = meta.location.filename() {
                out.push(format!("{}/{}", anchor, name));
            }
        }
        Ok(out)
    }

    async fn read_text_versioned(&self, uri: &str) -> Result<(String, String)> {
        let location = self.object_path(uri)?;
        let result = self
            .store
            .get(&location)
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?;
        let etag = result.meta.e_tag.clone();
        let bytes = result
            .bytes()
            .await
            .map_err(|err| storage_backend_error("read", uri, err))?;
        // The token SOURCE must agree with the write_text_if_match strategy
        // below: conditional-update backends compare ETags server-side, so
        // the token is the ETag; the local emulation compares content, so
        // the token is the content hash. Mixing them makes every CAS lose.
        let version = if self.supports_conditional_update {
            // Every S3-compatible store we target returns ETags; fall back
            // to a content token rather than failing if one ever omits it.
            etag.unwrap_or_else(|| local_version_token(&bytes))
        } else {
            local_version_token(&bytes)
        };
        let text = String::from_utf8(bytes.to_vec()).map_err(|err| {
            OmniError::manifest_internal(format!("storage read failed for '{}': {}", uri, err))
        })?;
        Ok((text, version))
    }

    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> Result<Option<String>> {
        let location = self.object_path(uri)?;
        if self.supports_conditional_update {
            let mode = PutMode::Update(object_store::UpdateVersion {
                e_tag: Some(expected_version.to_string()),
                version: None,
            });
            return match self
                .store
                .put_opts(
                    &location,
                    PutPayload::from(contents.as_bytes().to_vec()),
                    mode.into(),
                )
                .await
            {
                Ok(result) => Ok(Some(
                    result
                        .e_tag
                        .unwrap_or_else(|| local_version_token(contents.as_bytes())),
                )),
                Err(object_store::Error::Precondition { .. })
                | Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(err) => Err(storage_backend_error("write_if_match", uri, err)),
            };
        }
        // Local emulation: content-compare then atomic replace. NOT a
        // cross-process CAS (check-then-act gap) — safe under the callers'
        // lock protocol only; tracked in docs/dev/invariants.md Known Gaps.
        let current = match self.store.get(&location).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|err| storage_backend_error("read", uri, err))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(err) => return Err(storage_backend_error("read", uri, err)),
        };
        if local_version_token(&current) != expected_version {
            return Ok(None);
        }
        self.store
            .put(&location, PutPayload::from(contents.as_bytes().to_vec()))
            .await
            .map_err(|err| storage_backend_error("write_if_match", uri, err))?;
        Ok(Some(local_version_token(contents.as_bytes())))
    }

    async fn delete_prefix(&self, prefix_uri: &str) -> Result<()> {
        // Directories are a local-FS concept: a list+delete loop would
        // leave empty directory skeletons that local existence probes
        // (cluster graph_root_exists uses std Path::exists) report as
        // still-present. remove_dir_all reclaims them in one call.
        if self.codec == UriCodec::Local {
            let path = absolutize_lexically(local_path_from_uri(prefix_uri)?)?;
            return match tokio::fs::remove_dir_all(&path).await {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err.into()),
            };
        }
        let prefix = self.object_path(prefix_uri.trim_end_matches('/'))?;
        let mut entries = self.store.list(Some(&prefix));
        let mut locations = Vec::new();
        while let Some(meta) = entries
            .try_next()
            .await
            .map_err(|err| storage_backend_error("delete_prefix", prefix_uri, err))?
        {
            locations.push(meta.location);
        }
        for location in locations {
            match self.store.delete(&location).await {
                Ok(()) => {}
                Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(storage_backend_error("delete_prefix", prefix_uri, err)),
            }
        }
        Ok(())
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
        StorageKind::Local => Ok(Arc::new(ObjectStorageAdapter::local())),
        StorageKind::S3 => Ok(Arc::new(ObjectStorageAdapter::s3_from_root_uri(uri)?)),
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

/// Lexically absolutize a local path: join relative paths onto the current
/// working directory and fold `.` / `..` components, without touching the
/// filesystem. Required because `object_store::path::Path` rejects
/// relative and dot segments, while callers (the CLI in particular) pass
/// paths like `./graph.omni` verbatim.
fn absolutize_lexically(path: PathBuf) -> Result<PathBuf> {
    let joined = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map_err(|err| {
                OmniError::manifest_internal(format!(
                    "cannot resolve relative storage path '{}': {}",
                    path.display(),
                    err
                ))
            })?
            .join(path)
    };
    let mut out = PathBuf::new();
    for component in joined.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    Ok(out)
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

    /// The executable backend contract: every assertion here must hold for
    /// EVERY backend (the divergence class this adapter closed was "two
    /// implementations, one prose contract, no referee"). The S3 variant
    /// runs bucket-gated in `tests/s3_storage.rs`
    /// (`s3_adapter_conditional_writes_contract`).
    async fn contract_suite(adapter: &dyn StorageAdapter, root: &str) {
        // Write/read round-trip; replace is in-place and atomic.
        let a = format!("{root}/contract/a.json");
        adapter.write_text(&a, "v1").await.unwrap();
        assert_eq!(adapter.read_text(&a).await.unwrap(), "v1");
        adapter.write_text(&a, "v2").await.unwrap();
        assert_eq!(adapter.read_text(&a).await.unwrap(), "v2");

        // exists: object yes; missing no; non-empty prefix yes (the
        // directory-shaped probe Lance dataset roots rely on).
        assert!(adapter.exists(&a).await.unwrap());
        assert!(
            !adapter
                .exists(&format!("{root}/contract/missing.json"))
                .await
                .unwrap()
        );
        assert!(adapter.exists(&format!("{root}/contract")).await.unwrap());

        // if_absent: exactly one claim wins; the loser leaves the winner's
        // object untouched.
        let claim = format!("{root}/contract/claim.json");
        assert!(adapter.write_text_if_absent(&claim, "first").await.unwrap());
        assert!(!adapter.write_text_if_absent(&claim, "second").await.unwrap());
        assert_eq!(adapter.read_text(&claim).await.unwrap(), "first");

        // Versioned CAS: fresh token wins, stale token loses with Ok(None)
        // (never a silent overwrite), missing object can't match.
        let state = format!("{root}/contract/state.json");
        adapter.write_text(&state, "s1").await.unwrap();
        let (text, v1) = adapter.read_text_versioned(&state).await.unwrap();
        assert_eq!(text, "s1");
        let v2 = adapter
            .write_text_if_match(&state, "s2", &v1)
            .await
            .unwrap()
            .expect("fresh token must win");
        assert_ne!(v2, v1);
        assert!(
            adapter
                .write_text_if_match(&state, "s3", &v1)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(adapter.read_text(&state).await.unwrap(), "s2");
        assert!(
            adapter
                .write_text_if_match(&format!("{root}/contract/absent.json"), "x", &v1)
                .await
                .unwrap()
                .is_none()
        );

        // rename: destination is replaced; source is gone.
        let src = format!("{root}/contract/src.json");
        adapter.write_text(&src, "moved").await.unwrap();
        adapter.rename_text(&src, &a).await.unwrap();
        assert_eq!(adapter.read_text(&a).await.unwrap(), "moved");
        assert!(!adapter.exists(&src).await.unwrap());

        // list_dir: direct children only, no sibling-prefix bleed, output
        // URIs round-trip verbatim into read_text, missing dir is empty.
        let dir_uri = format!("{root}/contract/list");
        adapter
            .write_text(&format!("{dir_uri}/one.json"), "1")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{dir_uri}/two.json"), "2")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{dir_uri}/sub/three.json"), "3")
            .await
            .unwrap();
        adapter
            .write_text(&format!("{root}/contract/list_log/x.json"), "x")
            .await
            .unwrap();
        let mut listed = adapter.list_dir(&dir_uri).await.unwrap();
        listed.sort();
        assert_eq!(
            listed,
            vec![
                format!("{dir_uri}/one.json"),
                format!("{dir_uri}/two.json")
            ]
        );
        for uri in &listed {
            adapter.read_text(uri).await.unwrap();
        }
        assert!(
            adapter
                .list_dir(&format!("{root}/contract/nope"))
                .await
                .unwrap()
                .is_empty()
        );

        // delete: idempotent.
        adapter.delete(&claim).await.unwrap();
        adapter.delete(&claim).await.unwrap();
        assert!(!adapter.exists(&claim).await.unwrap());

        // delete_prefix: recursive + idempotent; nothing under the prefix
        // (including local directory skeletons) survives.
        adapter
            .delete_prefix(&format!("{root}/contract"))
            .await
            .unwrap();
        assert!(!adapter.exists(&a).await.unwrap());
        assert!(!adapter.exists(&format!("{root}/contract")).await.unwrap());
        adapter
            .delete_prefix(&format!("{root}/contract"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn contract_suite_local() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        contract_suite(&adapter, dir.path().to_str().unwrap()).await;
    }

    #[tokio::test]
    async fn contract_suite_in_memory() {
        // InMemory implements true conditional updates, so this runs the
        // strong-CAS path (ETag tokens + PutMode::Update) without a bucket.
        let adapter = ObjectStorageAdapter::in_memory();
        contract_suite(&adapter, "mem-root").await;
    }

    /// `write_text_if_absent` must make the contents visible to any
    /// subsequent reader before it returns — callers acknowledge
    /// success the moment it resolves (cluster state bootstrap reads
    /// the file back; init ownership claims depend on it).
    /// Regression: the previous hand-rolled local adapter wrote through a
    /// buffered `tokio::fs::File` without flushing, so the bytes could
    /// still be in flight on the blocking pool while a reader saw an empty
    /// or partial file. Reads back through `std::fs` deliberately —
    /// cross-API visibility is the point.
    #[tokio::test]
    async fn local_write_text_if_absent_is_read_visible_on_return() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let payload = "x".repeat(8 * 1024);
        for i in 0..1000 {
            let path = dir.path().join(format!("obj-{i}.json"));
            let uri = format!("{}", path.display());
            assert!(adapter.write_text_if_absent(&uri, &payload).await.unwrap());
            let read = std::fs::read_to_string(&path).unwrap();
            assert_eq!(
                read.len(),
                payload.len(),
                "iteration {i}: write_text_if_absent returned before its \
                 contents reached the file"
            );
        }
    }

    /// Regression for the write_text_if_absent buffering bug, via the
    /// `storage_for_uri` + `file://` construction path and a multi-thread
    /// runtime (complements `local_write_text_if_absent_is_read_visible_-
    /// on_return`, which uses the direct constructor and plain paths): a
    /// reader immediately after Ok(true) must never see the created file
    /// empty or short.
    #[tokio::test(flavor = "multi_thread")]
    async fn write_text_if_absent_is_read_consistent_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = storage_for_uri(&format!("file://{}", dir.path().display())).unwrap();
        let payload = "x".repeat(64 * 1024);
        for i in 0..200 {
            let uri = format!("file://{}/f{}.json", dir.path().display(), i);
            assert!(adapter.write_text_if_absent(&uri, &payload).await.unwrap());
            let read = std::fs::read_to_string(dir.path().join(format!("f{i}.json"))).unwrap();
            assert_eq!(read.len(), payload.len(), "iteration {i}: short read");
        }
    }

    /// Object-store semantics on the local filesystem: only objects exist.
    /// An empty directory is not an object and not a non-empty prefix —
    /// callers that genuinely probe local directories use std::fs.
    #[tokio::test]
    async fn local_exists_is_object_semantics_for_directories() {
        let dir = tempfile::tempdir().unwrap();
        let probe = dir.path().join("maybe-dataset");
        let adapter = ObjectStorageAdapter::local();
        std::fs::create_dir(&probe).unwrap();
        assert!(
            !adapter.exists(probe.to_str().unwrap()).await.unwrap(),
            "an empty directory is not an object"
        );
        std::fs::write(probe.join("1.manifest"), "m").unwrap();
        assert!(
            adapter.exists(probe.to_str().unwrap()).await.unwrap(),
            "a non-empty prefix exists (the Lance dataset-root probe shape)"
        );
    }

    /// list_dir output is anchored on the INPUT dir_uri, so `file://`
    /// anchors and paths with spaces round-trip byte-identically into
    /// read_text — the cluster store passes file://-schemed roots.
    #[tokio::test]
    async fn local_list_round_trips_file_scheme_and_spaces() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("with space");
        let adapter = ObjectStorageAdapter::local();
        let plain = format!("{}/x.json", root.display());
        adapter.write_text(&plain, "x").await.unwrap();

        let listed = adapter.list_dir(root.to_str().unwrap()).await.unwrap();
        assert_eq!(listed, vec![plain.clone()]);
        assert_eq!(adapter.read_text(&listed[0]).await.unwrap(), "x");

        let file_anchor = format!("file://{}", root.display());
        let listed = adapter.list_dir(&file_anchor).await.unwrap();
        assert_eq!(listed, vec![format!("{file_anchor}/x.json")]);
        assert_eq!(adapter.read_text(&listed[0]).await.unwrap(), "x");
    }

    /// Relative and dot-segment paths are lexically absolutized before
    /// hitting the object-path layer (which rejects them) — the CLI passes
    /// `./graph.omni`-shaped URIs verbatim.
    #[tokio::test]
    async fn local_paths_with_dot_segments_are_absolutized() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let uri = format!("{}/sub/../dotted.json", dir.path().display());
        adapter.write_text(&uri, "x").await.unwrap();
        assert_eq!(adapter.read_text(&uri).await.unwrap(), "x");
        assert!(dir.path().join("dotted.json").exists());
    }

    /// Upstream local rename creates missing destination parents — more
    /// lenient than the previous bare fs::rename; pinned so an upstream
    /// regression is loud.
    #[tokio::test]
    async fn local_rename_creates_missing_destination_parents() {
        let dir = tempfile::tempdir().unwrap();
        let adapter = ObjectStorageAdapter::local();
        let src = format!("{}/src.json", dir.path().display());
        adapter.write_text(&src, "x").await.unwrap();
        let dst = format!("{}/new-sub/dst.json", dir.path().display());
        adapter.rename_text(&src, &dst).await.unwrap();
        assert_eq!(adapter.read_text(&dst).await.unwrap(), "x");
    }

    #[test]
    fn storage_backend_selection_is_scheme_aware() {
        assert_eq!(storage_kind_for_uri("/tmp/graph"), StorageKind::Local);
        assert_eq!(
            storage_kind_for_uri("file:///tmp/graph"),
            StorageKind::Local
        );
        assert_eq!(
            storage_kind_for_uri("s3://omnigraph-preview/graph"),
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
        let location = parse_s3_uri("s3://bucket/graph/_schema.pg").unwrap();
        assert_eq!(location.bucket, "bucket");
        assert_eq!(location.key, "graph/_schema.pg");
    }

}
