//! Persisted cluster-state lock ownership.
//!
//! This is cluster control-plane infrastructure, not graph-engine authority.
//! Keeping it beside the cluster store avoids a leaf crate whose remaining
//! purpose would otherwise be one lock file.

use std::process;
use std::sync::Arc;

use omnigraph_storage::{
    StorageAdapter, StorageError, StorageHandle, StorageKind, normalize_root_uri,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

const LOCK_VERSION: u32 = 1;
const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";

#[derive(Debug, Error)]
pub(crate) enum StateLockError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("could not encode cluster state lock: {0}")]
    LockEncode(serde_json::Error),
    #[error("could not parse cluster state lock: {0}")]
    LockParse(serde_json::Error),
    #[error("unsupported cluster state lock version {0}; expected 1")]
    LockVersion(u32),
    #[error("invalid cluster state lock binding: {0}")]
    InvalidBinding(String),
}

/// Exact persisted `__cluster/lock.json` wire shape.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StateLockFile {
    version: u32,
    lock_id: String,
    operation: String,
    created_at: String,
    pid: u32,
}

impl StateLockFile {
    fn new(operation: &str) -> Self {
        Self {
            version: LOCK_VERSION,
            lock_id: Ulid::new().to_string(),
            operation: operation.to_owned(),
            created_at: OffsetDateTime::now_utc()
                .format(&Rfc3339)
                .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned()),
            pid: process::id(),
        }
    }

    pub(crate) fn parse(text: &str) -> Result<Self, StateLockError> {
        let lock: Self = serde_json::from_str(text).map_err(StateLockError::LockParse)?;
        if lock.version != LOCK_VERSION {
            return Err(StateLockError::LockVersion(lock.version));
        }
        Ok(lock)
    }

    pub(crate) fn lock_id(&self) -> &str {
        &self.lock_id
    }

    pub(crate) fn operation(&self) -> &str {
        &self.operation
    }

    pub(crate) fn created_at(&self) -> &str {
        &self.created_at
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }
}

/// Result of storage-native conditional lock acquisition.
#[derive(Debug)]
pub(crate) enum StateLockAcquire {
    Acquired(StateLockGuard),
    Held,
}

/// Exclusive persisted cluster-state lock.
///
/// Private fields and the absence of `Clone` make ownership non-forgeable.
/// Constructors perform atomic create-if-absent or the bounded initial
/// bootstrap owner's exact-version conditional transfer.
#[derive(Debug)]
pub(crate) struct StateLockGuard {
    adapter: Arc<dyn StorageAdapter>,
    uri: String,
    kind: StorageKind,
    lock: StateLockFile,
    release_on_drop: bool,
}

impl StateLockGuard {
    pub(crate) fn lock_id(&self) -> &str {
        self.lock.lock_id()
    }

    /// V2 admission must survive cancellation and process abandonment. Only
    /// an explicit, settled release may remove its persisted exclusion.
    pub(crate) fn hold_on_drop(&mut self) {
        self.release_on_drop = false;
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
        match self.kind {
            StorageKind::Local => {
                let path = self.uri.trim_start_matches("file://");
                let still_owned = std::fs::read_to_string(path)
                    .ok()
                    .and_then(|text| StateLockFile::parse(&text).ok())
                    .is_some_and(|lock| lock.lock_id() == self.lock_id());
                if still_owned {
                    let _ = std::fs::remove_file(path);
                }
            }
            StorageKind::S3 | StorageKind::Azure => {
                let adapter = Arc::clone(&self.adapter);
                let uri = self.uri.clone();
                let lock_id = self.lock_id().to_string();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                        tokio::task::block_in_place(move || {
                            handle.block_on(async move {
                                release_remote_lock_if_owned(&adapter, &uri, &lock_id).await;
                            });
                        });
                    } else {
                        handle.spawn(async move {
                            release_remote_lock_if_owned(&adapter, &uri, &lock_id).await;
                        });
                    }
                }
            }
        }
    }
}

async fn release_remote_lock_if_owned(adapter: &Arc<dyn StorageAdapter>, uri: &str, lock_id: &str) {
    let still_owned = adapter
        .read_text_if_exists(uri)
        .await
        .ok()
        .flatten()
        .and_then(|text| StateLockFile::parse(&text).ok())
        .is_some_and(|lock| lock.lock_id() == lock_id);
    if still_owned {
        let _ = adapter.delete(uri).await;
    }
}

pub(crate) async fn acquire_state_lock(
    storage: &StorageHandle,
    lock_uri: &str,
    operation: &str,
) -> Result<StateLockAcquire, StateLockError> {
    let _validated_cluster_root = cluster_root_from_lock_uri(lock_uri)?;
    let adapter = storage.adapter();
    let lock = StateLockFile::new(operation);
    let payload = serde_json::to_string_pretty(&lock).map_err(StateLockError::LockEncode)?;
    if adapter.write_text_if_absent(lock_uri, &payload).await? {
        return Ok(StateLockAcquire::Acquired(StateLockGuard {
            adapter,
            uri: lock_uri.to_string(),
            kind: storage.kind(),
            lock,
            release_on_drop: true,
        }));
    }
    Ok(StateLockAcquire::Held)
}

const BOOTSTRAP_OPERATION: &str = "bootstrap_serving";

/// No release path exists in initial bootstrap, including a lost write
/// acknowledgement before a guard could be constructed.
pub(crate) async fn acquire_bootstrap_lock(
    adapter: Arc<dyn StorageAdapter>,
    kind: StorageKind,
    uri: &str,
) -> Result<(StateLockGuard, String), StateLockError> {
    require_conditional_bootstrap(kind, uri)?;
    let lock = StateLockFile::new(BOOTSTRAP_OPERATION);
    let payload = serde_json::to_string_pretty(&lock).map_err(StateLockError::LockEncode)?;
    if !adapter.write_text_if_absent(uri, &payload).await? {
        return Err(StateLockError::InvalidBinding(
            "bootstrap lock is already held".into(),
        ));
    }
    let guard = StateLockGuard {
        adapter: adapter.clone(),
        uri: uri.into(),
        kind,
        lock,
        release_on_drop: false,
    };
    let (observed, version) = read_bootstrap_lock(&adapter, uri).await?;
    if observed.lock_id != guard.lock_id() {
        return Err(StateLockError::InvalidBinding(
            "bootstrap owner changed".into(),
        ));
    }
    Ok((guard, version))
}

fn require_conditional_bootstrap(kind: StorageKind, uri: &str) -> Result<(), StateLockError> {
    cluster_root_from_lock_uri(uri)?;
    if kind != StorageKind::S3 {
        return Err(StateLockError::InvalidBinding(
            "bootstrap handoff requires S3 conditional updates".into(),
        ));
    }
    Ok(())
}

async fn read_bootstrap_lock(
    adapter: &Arc<dyn StorageAdapter>,
    uri: &str,
) -> Result<(StateLockFile, String), StateLockError> {
    let (text, version) = adapter
        .read_text_versioned_if_exists_bounded(uri, 64 * 1024)
        .await?
        .ok_or_else(|| StateLockError::InvalidBinding("bootstrap lock is absent".into()))?;
    let lock = StateLockFile::parse(&text)?;
    if lock.operation != BOOTSTRAP_OPERATION
        || version.is_empty()
        || version.len() > 1024
        || version.chars().any(char::is_control)
    {
        return Err(StateLockError::InvalidBinding(
            "not a retained bootstrap lock and version".into(),
        ));
    }
    Ok((lock, version))
}

pub(crate) async fn verify_bootstrap_lock(
    adapter: &Arc<dyn StorageAdapter>,
    uri: &str,
    lock_id: &str,
    expected_version: &str,
) -> Result<(), StateLockError> {
    let (lock, version) = read_bootstrap_lock(adapter, uri).await?;
    if lock.lock_id != lock_id || version != expected_version {
        return Err(StateLockError::InvalidBinding(
            "bootstrap lock identity or version changed".into(),
        ));
    }
    Ok(())
}

pub(crate) async fn claim_bootstrap_lock(
    adapter: Arc<dyn StorageAdapter>,
    kind: StorageKind,
    uri: &str,
    lock_id: &str,
    expected_version: &str,
) -> Result<StateLockGuard, StateLockError> {
    require_conditional_bootstrap(kind, uri)?;
    verify_bootstrap_lock(&adapter, uri, lock_id, expected_version).await?;
    let lock = StateLockFile::new("serve");
    let payload = serde_json::to_string_pretty(&lock).map_err(StateLockError::LockEncode)?;
    // Never refresh the token or adopt a winner after an uncertain response.
    if adapter
        .write_text_if_match(uri, &payload, expected_version)
        .await?
        .is_none()
    {
        return Err(StateLockError::InvalidBinding(
            "bootstrap lock claim lost its exact-version CAS".into(),
        ));
    }
    Ok(StateLockGuard {
        adapter,
        uri: uri.into(),
        kind,
        lock,
        release_on_drop: false,
    })
}

fn cluster_root_from_lock_uri(lock_uri: &str) -> Result<String, StateLockError> {
    let suffix = format!("/{CLUSTER_LOCK_FILE}");
    let root = lock_uri.strip_suffix(&suffix).ok_or_else(|| {
        StateLockError::InvalidBinding(format!(
            "state lock must be stored at '<cluster>/{CLUSTER_LOCK_FILE}'"
        ))
    })?;
    normalize_root_uri(root).map_err(StateLockError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_storage::storage_handle_for_uri;

    #[test]
    fn lock_wire_is_strict_and_versioned() {
        let valid = r#"{
            "version": 1,
            "lock_id": "01LOCK",
            "operation": "apply",
            "created_at": "2026-08-06T00:00:00Z",
            "pid": 42
        }"#;
        let parsed = StateLockFile::parse(valid).unwrap();
        assert_eq!(parsed.lock_id(), "01LOCK");
        assert_eq!(parsed.operation(), "apply");
        assert_eq!(parsed.pid(), 42);

        let future = valid.replace("\"version\": 1", "\"version\": 2");
        assert!(matches!(
            StateLockFile::parse(&future),
            Err(StateLockError::LockVersion(2))
        ));
        let unknown = valid.replace("\"pid\": 42", "\"pid\": 42, \"extra\": true");
        assert!(matches!(
            StateLockFile::parse(&unknown),
            Err(StateLockError::LockParse(_))
        ));
    }

    #[tokio::test]
    async fn state_lock_is_exclusive_and_releases_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let storage = storage_handle_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/__cluster/lock.json");
        let guard = match acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap()
        {
            StateLockAcquire::Acquired(guard) => guard,
            StateLockAcquire::Held => panic!("fresh lock unexpectedly held"),
        };
        assert_eq!(guard.lock.operation(), "apply");
        assert_eq!(guard.uri, lock_uri);
        assert_eq!(
            cluster_root_from_lock_uri(&lock_uri).unwrap(),
            normalize_root_uri(&root).unwrap()
        );
        assert!(!guard.lock_id().is_empty());

        let lock_path = dir.path().join("__cluster/lock.json");
        let persisted = std::fs::read_to_string(&lock_path).unwrap();
        let parsed = StateLockFile::parse(&persisted).unwrap();
        assert_eq!(parsed.lock_id(), guard.lock_id());
        assert_eq!(parsed.operation(), "apply");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&persisted)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            5
        );
        assert!(matches!(
            acquire_state_lock(&storage, &lock_uri, "apply")
                .await
                .unwrap(),
            StateLockAcquire::Held
        ));

        drop(guard);
        assert!(!lock_path.exists());
        assert!(matches!(
            acquire_state_lock(&storage, &lock_uri, "apply")
                .await
                .unwrap(),
            StateLockAcquire::Acquired(_)
        ));
    }
}
