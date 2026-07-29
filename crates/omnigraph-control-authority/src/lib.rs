//! Cycle-free control-plane authority primitives.
//!
//! This prerequisite moves the live cluster state lock below the
//! engine/cluster split. The leaf depends on the concrete shared storage
//! handle, never on the engine, cluster, server, or their caller-implemented
//! traits. Opaque apply/runtime/export capabilities are intentionally deferred
//! until their canonical revision, actor, runtime-lifetime, and manifest
//! profile fences are complete.

use std::process;

use omnigraph_storage::{StorageAdapter, StorageError, StorageHandle, StorageKind};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

const LOCK_VERSION: u32 = 1;

#[derive(Debug, Error)]
pub enum AuthorityError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("could not encode cluster state lock: {0}")]
    LockEncode(serde_json::Error),
    #[error("could not parse cluster state lock: {0}")]
    LockParse(serde_json::Error),
    #[error("unsupported cluster state lock version {0}; expected 1")]
    LockVersion(u32),
}

/// Exact persisted `__cluster/lock.json` wire shape.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateLockFile {
    version: u32,
    lock_id: String,
    operation: String,
    created_at: String,
    pid: u32,
}

impl StateLockFile {
    pub fn parse(text: &str) -> Result<Self, AuthorityError> {
        let lock: Self = serde_json::from_str(text).map_err(AuthorityError::LockParse)?;
        if lock.version != LOCK_VERSION {
            return Err(AuthorityError::LockVersion(lock.version));
        }
        Ok(lock)
    }

    pub fn lock_id(&self) -> &str {
        &self.lock_id
    }

    pub fn operation(&self) -> &str {
        &self.operation
    }

    pub fn created_at(&self) -> &str {
        &self.created_at
    }

    pub fn pid(&self) -> u32 {
        self.pid
    }
}

/// Result of the storage-native conditional-create lock acquisition.
#[derive(Debug)]
pub enum StateLockAcquire {
    Acquired(StateLockGuard),
    Held,
}

/// Exclusive persisted cluster-state lock.
///
/// Private fields and the lack of `Clone`, `Default`, `Serialize`, or
/// `Deserialize` make possession non-forgeable. The only constructor performs
/// the concrete backend's atomic create-if-absent operation.
#[derive(Debug)]
pub struct StateLockGuard {
    adapter: std::sync::Arc<dyn StorageAdapter>,
    uri: String,
    kind: StorageKind,
    lock: StateLockFile,
}

impl StateLockGuard {
    pub fn lock_id(&self) -> &str {
        self.lock.lock_id()
    }

    pub fn operation(&self) -> &str {
        self.lock.operation()
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl Drop for StateLockGuard {
    fn drop(&mut self) {
        match self.kind {
            // Preserve deterministic local release: cluster command tests and
            // callers observe the lock gone when the command returns.
            StorageKind::Local => {
                let path = self.uri.trim_start_matches("file://");
                let _ = std::fs::remove_file(path);
            }
            // A short-lived CLI must not exit before object-store deletion
            // completes. Current-thread runtimes cannot block_in_place, so
            // they retain the prior best-effort spawned fallback.
            StorageKind::S3 => {
                let adapter = self.adapter.clone();
                let uri = self.uri.clone();
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread {
                        tokio::task::block_in_place(move || {
                            handle.block_on(async move {
                                let _ = adapter.delete(&uri).await;
                            });
                        });
                    } else {
                        handle.spawn(async move {
                            let _ = adapter.delete(&uri).await;
                        });
                    }
                }
            }
        }
    }
}

/// Acquire an exclusive state lock through the concrete shared storage path.
pub async fn acquire_state_lock(
    storage: &StorageHandle,
    lock_uri: &str,
    operation: &str,
) -> Result<StateLockAcquire, AuthorityError> {
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
    let payload = serde_json::to_string_pretty(&lock).map_err(AuthorityError::LockEncode)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use omnigraph_storage::storage_handle_for_uri;

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
        assert_eq!(guard.operation(), "apply");
        assert_eq!(guard.uri(), lock_uri);
        assert!(!guard.lock_id().is_empty());
        assert!(dir.path().join("__cluster/lock.json").exists());
        let persisted = std::fs::read_to_string(dir.path().join("__cluster/lock.json")).unwrap();
        let parsed = StateLockFile::parse(&persisted).unwrap();
        assert_eq!(parsed.lock_id(), guard.lock_id());
        assert_eq!(parsed.operation(), "apply");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&persisted)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            5,
            "the persisted lock wire shape has exactly five fields"
        );

        assert!(matches!(
            acquire_state_lock(&storage, &lock_uri, "apply")
                .await
                .unwrap(),
            StateLockAcquire::Held
        ));

        drop(guard);
        assert!(!dir.path().join("__cluster/lock.json").exists());

        let reacquired = acquire_state_lock(&storage, &lock_uri, "apply")
            .await
            .unwrap();
        assert!(matches!(reacquired, StateLockAcquire::Acquired(_)));
    }
}
