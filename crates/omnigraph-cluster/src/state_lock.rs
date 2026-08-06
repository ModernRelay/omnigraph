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
/// The only constructor performs the backend's atomic create-if-absent.
#[derive(Debug)]
pub(crate) struct StateLockGuard {
    adapter: Arc<dyn StorageAdapter>,
    uri: String,
    kind: StorageKind,
    lock: StateLockFile,
}

impl StateLockGuard {
    pub(crate) fn lock_id(&self) -> &str {
        self.lock.lock_id()
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
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
            StorageKind::S3 => {
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
    let lock = StateLockFile {
        version: LOCK_VERSION,
        lock_id: Ulid::new().to_string(),
        operation: operation.to_string(),
        created_at: OffsetDateTime::now_utc()
            .format(&Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string()),
        pid: process::id(),
    };
    let payload = serde_json::to_string_pretty(&lock).map_err(StateLockError::LockEncode)?;
    if adapter.write_text_if_absent(lock_uri, &payload).await? {
        return Ok(StateLockAcquire::Acquired(StateLockGuard {
            adapter,
            uri: lock_uri.to_string(),
            kind: storage.kind(),
            lock,
        }));
    }
    Ok(StateLockAcquire::Held)
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
