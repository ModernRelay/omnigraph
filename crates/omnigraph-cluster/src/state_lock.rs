//! Persisted cluster-state lock ownership.
//!
//! This is cluster control-plane infrastructure, not graph-engine authority.
//! Keeping it beside the cluster store avoids a leaf crate whose remaining
//! purpose would otherwise be one lock file.

use std::process;
use std::sync::Arc;

use omnigraph_storage::{StorageAdapter, StorageError, StorageKind, normalize_root_uri};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use ulid::Ulid;

const LOCK_VERSION: u32 = 1;
const RELEASED_LOCK_VERSION: u32 = 2;
/// A held lock with a lease (`lease`): a successor may take it over after
/// its heartbeats stop. Older binaries refuse this version, so they neither
/// take it over nor release it.
const LEASED_LOCK_VERSION: u32 = 3;
const CLUSTER_LOCK_FILE: &str = "__cluster/lock.json";

#[derive(Debug, Error)]
pub(crate) enum StateLockError {
    #[error("{0}")]
    Storage(#[from] StorageError),
    #[error("could not encode cluster state lock: {0}")]
    LockEncode(serde_json::Error),
    #[error("could not parse cluster state lock: {0}")]
    LockParse(serde_json::Error),
    #[error("unsupported cluster state lock version {0}")]
    LockVersion(u32),
    #[error("invalid cluster state lock binding: {0}")]
    InvalidBinding(String),
    #[error(
        "the cluster lock's lease fenced before its first heartbeat was acknowledged ({0:?}); \
         this process must not serve, and a restart takes the lock over"
    )]
    LeaseFenced(crate::lease::LeaseFenced),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lease_seconds: Option<u32>,
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
            release_id: None,
            lease_seconds: None,
        }
    }

    /// A held lock of `kind`. A server's lock on S3 is leased: S3's if-match
    /// lets a successor replace a silent owner's record without racing that
    /// owner's release, and a server stops its storage mutations when its
    /// lease fences. Every other lock is unleased: local roots have no
    /// conditional update, Azure's admission wrapper owns that decision, and
    /// a CLI writer or deployment keeps its exact-ID unlock.
    fn held(operation: &str, kind: StorageKind, lease_seconds: u32) -> Self {
        let mut lock = Self::new(operation);
        if kind == StorageKind::S3 && operation == SERVE_OPERATION {
            lock.version = LEASED_LOCK_VERSION;
            lock.lease_seconds = Some(lease_seconds);
        }
        lock
    }

    pub(crate) fn lease_seconds(&self) -> Option<u32> {
        self.lease_seconds
    }

    pub(crate) fn parse(text: &str) -> Result<Self, StateLockError> {
        let lock: Self = serde_json::from_str(text).map_err(StateLockError::LockParse)?;
        match (lock.version, lock.release_id.as_deref(), lock.lease_seconds) {
            (LOCK_VERSION, None, None) => {}
            (RELEASED_LOCK_VERSION, Some(id), None) if Ulid::from_string(id).is_ok() => {}
            (LEASED_LOCK_VERSION, None, Some(seconds))
                if crate::lease::valid_lease_seconds(seconds) => {}
            (LOCK_VERSION | RELEASED_LOCK_VERSION | LEASED_LOCK_VERSION, _, _) => {
                return Err(StateLockError::InvalidBinding(
                    "held, leased and released lock shapes must match their wire version".into(),
                ));
            }
            (version, _, _) => return Err(StateLockError::LockVersion(version)),
        }
        Ok(lock)
    }

    pub(crate) fn is_released(&self) -> bool {
        self.release_id.is_some()
    }

    /// The released marker older binaries also read: version 2, no lease.
    fn mark_released(mut self) -> Self {
        self.version = RELEASED_LOCK_VERSION;
        self.release_id = Some(Ulid::new().to_string());
        self.lease_seconds = None;
        self
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
    /// Renews a leased lock while this guard lives.
    /// Boxed: a guard is moved through every acquisition result.
    lease: Option<Box<crate::lease::LeaseKeeper>>,
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

    /// Start renewing a leased lock through `lease_adapter` (`lease`); its
    /// record was written by a request sent at `lock_sent`. A guard of an
    /// unleased lock keeps none. A lease that fences before its first beat is
    /// acknowledged yields no guard: the lock stays for a successor.
    async fn start_lease(
        mut self,
        lease_adapter: Arc<dyn StorageAdapter>,
        lock_sent: tokio::time::Instant,
    ) -> Result<Self, StateLockError> {
        if let Some(seconds) = self.lock.lease_seconds() {
            match cluster_root_from_lock_uri(&self.uri) {
                Ok(root) => {
                    let keeper = crate::lease::LeaseKeeper::start(
                        lease_adapter,
                        &root,
                        self.lock.lock_id(),
                        seconds,
                        lock_sent,
                        self.adapter.io_scope(),
                    )
                    .await
                    .map_err(StateLockError::LeaseFenced)?;
                    self.lease = Some(Box::new(keeper));
                }
                // The error names the lock's path, which logs never carry.
                Err(_) => {
                    tracing::error!("leased lock has no cluster root; not renewing it");
                }
            }
        }
        Ok(self)
    }

    /// Resolves to `Some` once this guard's lease fenced; stays `None`
    /// while it holds, and for an unleased lock.
    pub(crate) fn lease_fenced(
        &self,
    ) -> Option<tokio::sync::watch::Receiver<Option<crate::lease::LeaseFenced>>> {
        self.lease.as_deref().map(crate::lease::LeaseKeeper::fenced)
    }

    /// Stop renewing before a clean release, and return the lease directory
    /// the release removes afterwards.
    pub(crate) async fn stop_lease(&mut self) -> Option<String> {
        let keeper = self.lease.take()?;
        let dir = keeper.dir().to_string();
        keeper.stop().await;
        Some(dir)
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

/// The serving admission: the one acquisition that takes over a silent
/// leased owner, so an orchestrator can restart a failed server. Every other
/// operation refuses a held lock at once, as an operator would.
const SERVE_OPERATION: &str = "serve";

/// Acquire the lock at `lock_uri` through `adapter`. A leased lock's
/// heartbeats, and a successor's watch and claim, go through
/// `lease_adapter`: the same root outside the owner's storage scope, with
/// the backend's own retries (`lease`).
pub(crate) async fn acquire_state_lock(
    adapter: Arc<dyn StorageAdapter>,
    lease_adapter: Arc<dyn StorageAdapter>,
    kind: StorageKind,
    lock_uri: &str,
    operation: &str,
) -> Result<StateLockAcquire, StateLockError> {
    acquire_state_lock_leased(
        adapter,
        lease_adapter,
        kind,
        lock_uri,
        operation,
        crate::lease::configured_lease_seconds(),
    )
    .await
}

async fn acquire_state_lock_leased(
    adapter: Arc<dyn StorageAdapter>,
    lease_adapter: Arc<dyn StorageAdapter>,
    kind: StorageKind,
    lock_uri: &str,
    operation: &str,
    lease_seconds: u32,
) -> Result<StateLockAcquire, StateLockError> {
    let root = cluster_root_from_lock_uri(lock_uri)?;
    let lock = StateLockFile::held(operation, kind, lease_seconds);
    let payload = serde_json::to_string_pretty(&lock).map_err(StateLockError::LockEncode)?;
    // When the request that makes this record visible was sent: a lease
    // counts its owner's first deadline from here (`lease`).
    let mut lock_sent = tokio::time::Instant::now();
    let created = adapter.write_text_if_absent(lock_uri, &payload).await?;
    let acquired = if created {
        true
    } else if kind == StorageKind::Azure {
        false
    } else {
        // Keep the key present across serving lifetimes on S3. A delayed
        // release or old create cannot overwrite the fresh owner. Never
        // refresh a failed CAS or adopt a write whose acknowledgement was
        // lost.
        let Some((text, version)) = adapter
            .read_text_versioned_if_exists_bounded(lock_uri, 64 * 1024)
            .await?
        else {
            return Ok(StateLockAcquire::Held);
        };
        let previous = StateLockFile::parse(&text)?;
        if previous.is_released() {
            lock_sent = tokio::time::Instant::now();
            kind == StorageKind::S3
                && adapter
                    .write_text_if_match(lock_uri, &payload, &version)
                    .await?
                    .is_some()
        } else if let Some(seconds) = previous
            .lease_seconds()
            .filter(|_| operation == SERVE_OPERATION && kind == StorageKind::S3)
        {
            match take_over(
                &adapter,
                lease_adapter.as_ref(),
                lock_uri,
                &root,
                &previous,
                &version,
                seconds,
                &lock,
                &payload,
            )
            .await?
            {
                Some(sent) => {
                    lock_sent = sent;
                    true
                }
                None => false,
            }
        } else {
            false
        }
    };
    if acquired {
        let guard = StateLockGuard {
            adapter,
            uri: lock_uri.to_string(),
            kind,
            lock,
            release_on_drop: kind != StorageKind::S3,
            lease: None,
        };
        return Ok(StateLockAcquire::Acquired(
            guard.start_lease(lease_adapter, lock_sent).await?,
        ));
    }
    Ok(StateLockAcquire::Held)
}

/// Take over an S3 leased lock whose owner has gone silent (`lease`): watch
/// its heartbeats, make sure the claim exists, then replace `lock.json` with
/// `lock` by if-match on the version first read. That if-match alone decides
/// the next owner: of two successors, or of a successor and the old owner's
/// clean release (if-match on the version it read), exactly one applies, and
/// the loser refuses. A replacement that does not apply leaves `lock.json` at
/// that version, so the next start takes it over the same way. Returns when
/// the replacement was sent, if it applied.
#[allow(clippy::too_many_arguments)]
async fn take_over(
    adapter: &Arc<dyn StorageAdapter>,
    lease_adapter: &dyn StorageAdapter,
    lock_uri: &str,
    root: &str,
    previous: &StateLockFile,
    version: &str,
    seconds: u32,
    lock: &StateLockFile,
    payload: &str,
) -> Result<Option<tokio::time::Instant>, StateLockError> {
    tracing::warn!(
        lock_id = previous.lock_id(),
        lease_seconds = seconds,
        "cluster lock is held by a leased owner; watching its heartbeats before taking it over"
    );
    let watched = crate::lease::watch_and_claim(
        lease_adapter,
        root,
        previous.lock_id(),
        seconds,
        lock.lock_id(),
    )
    .await?;
    if watched == crate::lease::Watch::Alive {
        return Ok(None);
    }
    let sent = tokio::time::Instant::now();
    let replaced = adapter
        .write_text_if_match(lock_uri, payload, version)
        .await?
        .is_some();
    if replaced {
        tracing::warn!(
            previous = previous.lock_id(),
            successor = lock.lock_id(),
            "took over a silent owner's cluster lock"
        );
        let _ = lease_adapter
            .delete_prefix(&crate::lease::lease_dir(root, previous.lock_id()))
            .await;
    }
    Ok(replaced.then_some(sent))
}

/// The caller must have settled every accepted effect and excluded competing
/// administrative unlocks. Remote clean release never deletes the lock key.
pub(crate) async fn release_settled_lock(
    adapter: &Arc<dyn StorageAdapter>,
    kind: StorageKind,
    uri: &str,
    lock_id: &str,
) -> Result<(), StateLockError> {
    let (text, version) = adapter
        .read_text_versioned_if_exists_bounded(uri, 64 * 1024)
        .await?
        .ok_or_else(|| StateLockError::InvalidBinding("held lock is absent".into()))?;
    let lock = StateLockFile::parse(&text)?;
    if lock.is_released() || lock.lock_id() != lock_id {
        return Err(StateLockError::InvalidBinding(
            "settled release no longer owns the exact held lock".into(),
        ));
    }
    match kind {
        // Azure preserves the explicit legacy settlement obligation and its
        // independent admission wrapper; server shutdown does not call this
        // as an automatic Azure release path.
        StorageKind::Local | StorageKind::Azure => adapter.delete(uri).await?,
        StorageKind::S3 => {
            let payload = serde_json::to_string_pretty(&lock.mark_released())
                .map_err(StateLockError::LockEncode)?;
            if adapter
                .write_text_if_match(uri, &payload, &version)
                .await?
                .is_none()
            {
                return Err(StateLockError::InvalidBinding(
                    "settled release lost its exact-version CAS".into(),
                ));
            }
        }
    }
    Ok(())
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
        lease: None,
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
    if lock.is_released()
        || lock.operation != BOOTSTRAP_OPERATION
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
    lease_adapter: Arc<dyn StorageAdapter>,
    kind: StorageKind,
    uri: &str,
    lock_id: &str,
    expected_version: &str,
) -> Result<StateLockGuard, StateLockError> {
    require_conditional_bootstrap(kind, uri)?;
    verify_bootstrap_lock(&adapter, uri, lock_id, expected_version).await?;
    let lock = StateLockFile::held(
        SERVE_OPERATION,
        kind,
        crate::lease::configured_lease_seconds(),
    );
    let payload = serde_json::to_string_pretty(&lock).map_err(StateLockError::LockEncode)?;
    let lock_sent = tokio::time::Instant::now();
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
    StateLockGuard {
        adapter,
        uri: uri.into(),
        kind,
        lock,
        release_on_drop: false,
        lease: None,
    }
    .start_lease(lease_adapter, lock_sent)
    .await
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

        let future = valid.replace("\"version\": 1", "\"version\": 4");
        assert!(matches!(
            StateLockFile::parse(&future),
            Err(StateLockError::LockVersion(4))
        ));
        // A leased lock is version 3 and records a lease within the bounds.
        let leased = valid
            .replace("\"version\": 1", "\"version\": 3")
            .replace("\"pid\": 42", "\"pid\": 42, \"lease_seconds\": 30");
        assert_eq!(
            StateLockFile::parse(&leased).unwrap().lease_seconds(),
            Some(30)
        );
        for refused in [
            valid.replace("\"version\": 1", "\"version\": 3"),
            leased.replace("\"lease_seconds\": 30", "\"lease_seconds\": 0"),
            leased.replace("\"lease_seconds\": 30", "\"lease_seconds\": 601"),
            leased.replace("\"version\": 3", "\"version\": 1"),
        ] {
            assert!(
                matches!(
                    StateLockFile::parse(&refused),
                    Err(StateLockError::InvalidBinding(_))
                ),
                "{refused}"
            );
        }
        let released_leased = StateLockFile::parse(&leased).unwrap().mark_released();
        assert_eq!(
            released_leased.lease_seconds(),
            None,
            "a release drops the lease"
        );
        let released = parsed.mark_released();
        let encoded = serde_json::to_string(&released).unwrap();
        assert!(StateLockFile::parse(&encoded).unwrap().is_released());
        assert!(StateLockFile::parse(&encoded.replace("\"version\":2", "\"version\":1")).is_err());
        assert!(StateLockFile::parse(&valid.replace("\"version\": 1", "\"version\": 2")).is_err());
        let unknown = valid.replace("\"pid\": 42", "\"pid\": 42, \"extra\": true");
        assert!(matches!(
            StateLockFile::parse(&unknown),
            Err(StateLockError::LockParse(_))
        ));
    }

    /// A serving owner that dies keeps its lock, and its heartbeats stop. A
    /// restarted server refuses while a live owner beats, then takes over a
    /// silent owner's lock after a lease and a half, with no unlock; an
    /// operation other than serving refuses a held lock at once. The store
    /// is in memory with S3's conditional semantics, the one backend whose
    /// locks are leased.
    #[tokio::test]
    async fn a_restarted_server_takes_over_a_silent_owners_lock_issue_1067() {
        const LEASE: u32 = 2;
        let store: Arc<dyn StorageAdapter> =
            Arc::new(omnigraph_storage::ObjectStorageAdapter::in_memory());
        let lock_uri = "/lease-tests/__cluster/lock.json";
        let root = cluster_root_from_lock_uri(lock_uri).unwrap();
        let acquire = |operation: &'static str| {
            acquire_state_lock_leased(
                store.clone(),
                store.clone(),
                StorageKind::S3,
                lock_uri,
                operation,
                LEASE,
            )
        };
        let StateLockAcquire::Acquired(owner) = acquire("serve").await.unwrap() else {
            panic!("a fresh root is free");
        };
        assert_eq!(owner.lock.lease_seconds(), Some(LEASE));
        let started = tokio::time::Instant::now();
        assert!(matches!(
            acquire("serve").await.unwrap(),
            StateLockAcquire::Held
        ));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(u64::from(LEASE)),
            "a live owner's next beat sends the successor away"
        );
        assert!(matches!(
            acquire("graph_operation").await.unwrap(),
            StateLockAcquire::Held
        ));

        // The owner's process ends: no release, no more beats.
        let previous = owner.lock_id().to_string();
        drop(owner);
        let started = tokio::time::Instant::now();
        let StateLockAcquire::Acquired(successor) = acquire("serve").await.unwrap() else {
            panic!("a silent owner's lock is taken over");
        };
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(u64::from(LEASE)) * 3 / 2,
            "takeover waits out a lease and a half of silence"
        );
        let stored = StateLockFile::parse(&store.read_text(lock_uri).await.unwrap()).unwrap();
        assert_eq!(stored.lock_id(), successor.lock_id());
        assert_ne!(stored.lock_id(), previous);
        assert!(
            store
                .list_dir(&crate::lease::lease_dir(&root, &previous))
                .await
                .unwrap()
                .is_empty(),
            "the superseded owner's beats are removed"
        );
    }

    /// A successor that claimed and then stopped before its if-match leaves
    /// the claim and the dead owner's record. The next start still takes
    /// the lock over after its own silence, and of two starts racing, the
    /// if-match picks exactly one.
    #[tokio::test]
    async fn a_takeover_survives_a_successor_that_stopped_after_its_claim_issue_1067() {
        const LEASE: u32 = 2;
        let store: Arc<dyn StorageAdapter> =
            Arc::new(omnigraph_storage::ObjectStorageAdapter::in_memory());
        let lock_uri = "/lease-tests/__cluster/lock.json";
        let root = cluster_root_from_lock_uri(lock_uri).unwrap();
        let acquire = || {
            acquire_state_lock_leased(
                store.clone(),
                store.clone(),
                StorageKind::S3,
                lock_uri,
                SERVE_OPERATION,
                LEASE,
            )
        };
        let StateLockAcquire::Acquired(owner) = acquire().await.unwrap() else {
            panic!("a fresh root is free");
        };
        let previous = owner.lock_id().to_string();
        drop(owner);
        // A successor found the owner silent and claimed, then stopped
        // before replacing `lock.json`.
        assert_eq!(
            crate::lease::watch_and_claim(store.as_ref(), &root, &previous, LEASE, "stopped")
                .await
                .unwrap(),
            crate::lease::Watch::Silent
        );
        let started = tokio::time::Instant::now();
        let (first, second) = tokio::join!(acquire(), acquire());
        assert!(
            started.elapsed() >= std::time::Duration::from_secs(u64::from(LEASE)) * 3 / 2,
            "each start waits out its own silence"
        );
        let acquired: Vec<StateLockGuard> = [first.unwrap(), second.unwrap()]
            .into_iter()
            .filter_map(|outcome| match outcome {
                StateLockAcquire::Acquired(guard) => Some(guard),
                StateLockAcquire::Held => None,
            })
            .collect();
        assert_eq!(acquired.len(), 1, "the if-match picks exactly one");
        let stored = StateLockFile::parse(&store.read_text(lock_uri).await.unwrap()).unwrap();
        assert_eq!(stored.lock_id(), acquired[0].lock_id());
        assert_ne!(stored.lock_id(), previous);
    }

    /// The old owner's clean release and a successor's replacement are both
    /// if-match on the version each read, so the one that lands first wins
    /// and the other refuses: a release cannot remove a successor's record.
    #[tokio::test]
    async fn a_release_and_a_takeover_never_both_apply_issue_1067() {
        let store: Arc<dyn StorageAdapter> =
            Arc::new(omnigraph_storage::ObjectStorageAdapter::in_memory());
        let lock_uri = "/lease-tests/__cluster/lock.json";
        let StateLockAcquire::Acquired(owner) = acquire_state_lock_leased(
            store.clone(),
            store.clone(),
            StorageKind::S3,
            lock_uri,
            "serve",
            2,
        )
        .await
        .unwrap() else {
            panic!("a fresh root is free");
        };
        let (_, read_by_owner) = store
            .read_text_versioned_if_exists_bounded(lock_uri, 64 * 1024)
            .await
            .unwrap()
            .unwrap();
        // A successor replaced the record after the owner read it.
        let successor = StateLockFile::held("serve", StorageKind::S3, 2);
        let replaced = serde_json::to_string_pretty(&successor).unwrap();
        assert!(
            store
                .write_text_if_match(lock_uri, &replaced, &read_by_owner)
                .await
                .unwrap()
                .is_some()
        );
        let released =
            serde_json::to_string_pretty(&StateLockFile::parse(&replaced).unwrap().mark_released())
                .unwrap();
        assert!(
            store
                .write_text_if_match(lock_uri, &released, &read_by_owner)
                .await
                .unwrap()
                .is_none(),
            "the owner's release against the version it read refuses"
        );
        assert!(
            release_settled_lock(&store, StorageKind::S3, lock_uri, owner.lock_id())
                .await
                .is_err(),
            "a settled release no longer owns the lock"
        );
        let stored = StateLockFile::parse(&store.read_text(lock_uri).await.unwrap()).unwrap();
        assert_eq!(stored.lock_id(), successor.lock_id());
        assert!(!stored.is_released());
    }

    /// Only a server's lock on S3 is leased. A local root has no conditional
    /// update, Azure's admission wrapper owns its own decision, and a CLI
    /// writer or deployment has no fence that would stop its writes.
    #[test]
    fn only_a_servers_s3_lock_is_leased_issue_1067() {
        for (operation, kind, leased) in [
            (SERVE_OPERATION, StorageKind::S3, true),
            ("apply", StorageKind::S3, false),
            ("graph_operation", StorageKind::S3, false),
            (SERVE_OPERATION, StorageKind::Local, false),
            (SERVE_OPERATION, StorageKind::Azure, false),
        ] {
            let lock = StateLockFile::held(operation, kind, 30);
            assert_eq!(
                lock.lease_seconds().is_some(),
                leased,
                "{operation} on {kind:?}"
            );
        }
    }

    /// A local lock is never taken over, even one whose record carries a
    /// lease: replacing it by overwrite could be undone by a release delete
    /// the old owner had already sent. A server refuses it at once.
    #[tokio::test]
    async fn a_local_lock_is_never_taken_over_issue_1067() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let storage = storage_handle_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/__cluster/lock.json");
        let lock_path = dir.path().join("__cluster/lock.json");
        std::fs::create_dir_all(lock_path.parent().unwrap()).unwrap();
        let mut leased = StateLockFile::new(SERVE_OPERATION);
        leased.version = LEASED_LOCK_VERSION;
        leased.lease_seconds = Some(2);
        let record = serde_json::to_string_pretty(&leased).unwrap();
        std::fs::write(&lock_path, &record).unwrap();
        let started = std::time::Instant::now();
        assert!(matches!(
            acquire_state_lock_leased(
                storage.adapter(),
                storage.adapter(),
                storage.kind(),
                &lock_uri,
                SERVE_OPERATION,
                2,
            )
            .await
            .unwrap(),
            StateLockAcquire::Held
        ));
        assert!(started.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(std::fs::read_to_string(&lock_path).unwrap(), record);
    }

    #[tokio::test]
    async fn state_lock_is_exclusive_and_releases_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let root = format!("file://{}", dir.path().display());
        let storage = storage_handle_for_uri(&root).unwrap();
        let lock_uri = format!("{root}/__cluster/lock.json");
        let guard = match acquire_state_lock(
            storage.adapter(),
            storage.adapter(),
            storage.kind(),
            &lock_uri,
            "apply",
        )
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
            5,
            "a local lock records no lease"
        );
        assert_eq!(parsed.lease_seconds(), None);
        // A local root has no conditional update to replace a record with,
        // so even a server refuses a held local lock at once.
        for operation in ["apply", SERVE_OPERATION] {
            let started = std::time::Instant::now();
            assert!(matches!(
                acquire_state_lock(
                    storage.adapter(),
                    storage.adapter(),
                    storage.kind(),
                    &lock_uri,
                    operation
                )
                .await
                .unwrap(),
                StateLockAcquire::Held
            ));
            assert!(started.elapsed() < std::time::Duration::from_secs(1));
        }

        drop(guard);
        assert!(!lock_path.exists());
        assert!(matches!(
            acquire_state_lock(
                storage.adapter(),
                storage.adapter(),
                storage.kind(),
                &lock_uri,
                "apply",
            )
            .await
            .unwrap(),
            StateLockAcquire::Acquired(_)
        ));
    }
}
