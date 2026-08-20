//! Per-`(table_key, branch)` writer queues.
//!
//! These queues are the engine's process-local, root-scoped write-serialization
//! mechanism. The server normally holds one lockless `Arc<Omnigraph>`, but
//! independently opened handles for the same canonical local root identity
//! (or the same opaque object-store URI) share this manager too. Legacy
//! writers serialize only on `(table_key, branch_ref)` so
//! disjoint keys can proceed concurrently. RFC-022-enrolled mutation/load
//! attempts additionally take a coarse branch effect gate because validation
//! may depend on tables they do not write. This module owns both queue classes;
//! callers in `MutationStaging::commit_all`, branch controls, `branch_merge`,
//! `schema_apply`, `ensure_indices`, cleanup, branch forking, and recovery acquire the applicable guards
//! before a Lance HEAD advance or destructive recovery action. Serialization
//! remains in-process only; cross-process writers on one graph remain
//! one-winner-CAS at publish.
//!
//! ## Why exclusive `tokio::sync::Mutex<()>` per key
//!
//! Lance's `Dataset::restore` "wins" against concurrent Append/Update/
//! Delete/CreateIndex/Merge per `check_restore_txn`, silently orphaning
//! the concurrent writer's commit. The queue's *only* application-layer
//! job is to serialize Restore against every other writer on the same
//! `(table_key, branch_ref)`. Lance OCC handles the rest of the conflict
//! matrix (Append vs Append fully compatible, Update vs Update rebases or
//! retries, etc.) but cannot make Restore symmetric — that's an upstream
//! design choice. Until Lance fixes Restore (or BatchCommitTables
//! changes the protocol), every writer takes the same exclusive lock.
//!
//! `RwLock` (shared for normal writes, exclusive for Restore) is the
//! natural follow-up but adds a writer-classification surface that's
//! easy to get wrong; misclassifying any writer reintroduces the
//! orphaning hazard. We start with `Mutex` and revisit based on
//! production telemetry.
//!
//! ## Sorted-order acquisition
//!
//! `acquire_many` accepts a slice of keys and acquires them in
//! lexicographic order. Multi-table writers and control paths (mutation
//! finalize, branch merge, schema apply, maintenance, and recovery) MUST go through
//! `acquire_many` so all callers agree on acquisition order — this is
//! how lock-order inversion deadlock is prevented.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard,
    RwLock as AsyncRwLock,
};

/// DST seam: one write-queue slot — the lock plus
/// its RELEASE EPOCH. The epoch exists because a first cut of the seam
/// measured a genuine arrival-order leak: the mutex RELEASE was an ungated
/// event, so a contender's retry could race the holder's guard drop and
/// same-seed runs diverged (grant-log flake, ~1 in 3). With the epoch,
/// releases are TURN-ORDERED (see `QueueGuard`) and a contender re-attempts
/// only after observing a release — ordered strictly after the unlock,
/// never racing it.
pub(crate) struct QueueSlot {
    lock: Arc<AsyncMutex<()>>,
    releases: std::sync::atomic::AtomicU64,
}

impl QueueSlot {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            lock: Arc::new(AsyncMutex::new(())),
            releases: std::sync::atomic::AtomicU64::new(0),
        })
    }
}

/// A held write-queue lock whose RELEASE is visible to the harness arbiter:
/// dropping it takes a turn (when the gate hook is installed), unlocks
/// INSIDE that turn, and bumps the slot's release epoch — so an unlock is a
/// scheduled event with a deterministic position in the grant sequence,
/// exactly like the acquisition attempts. Uninstalled: a plain unlock.
pub(crate) struct QueueGuard {
    inner: Option<OwnedMutexGuard<()>>,
    slot: Arc<QueueSlot>,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        // Turn first (None when uninstalled/unarmed — plain path), unlock
        // under it, bump the epoch last: a waiter that observes the bump
        // sees a lock that is genuinely free.
        let _turn = crate::dst_gate::turn();
        drop(self.inner.take());
        self.slot
            .releases
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Scheduled lock acquisition. Uninstalled (or hook declining): plain
/// blocking `lock_owned`. Installed: the contender STAYS IN THE TURN LOOP
/// the whole time — always visible and drawable at the arbiter (a
/// turn-free wait was measured to reproduce the invisibility disease as
/// systematic escapes) — but a granted turn ATTEMPTS the lock only when
/// the slot's release epoch has advanced past the last failed attempt
/// (epoch read UNDER the turn, so it is turn-ordered); otherwise the turn
/// is a deterministic no-op. Attempts therefore never race the unlock
/// (releases bump the epoch inside their own turns, see `QueueGuard`),
/// and every event — attempt, no-op, release — has a seeded position in
/// the grant sequence.
async fn scheduled_lock(slot: Arc<QueueSlot>) -> QueueGuard {
    let mut wait_epoch: Option<u64> = None;
    loop {
        match crate::dst_gate::turn() {
            None => {
                let guard = Arc::clone(&slot.lock).lock_owned().await;
                return QueueGuard {
                    inner: Some(guard),
                    slot,
                };
            }
            Some(_turn) => {
                let epoch_now = slot.releases.load(std::sync::atomic::Ordering::SeqCst);
                if wait_epoch.is_none_or(|e| epoch_now != e) {
                    match Arc::clone(&slot.lock).try_lock_owned() {
                        Ok(guard) => {
                            return QueueGuard {
                                inner: Some(guard),
                                slot: Arc::clone(&slot),
                            };
                        }
                        Err(_) => wait_epoch = Some(epoch_now),
                    }
                }
                // else: no release since the failed attempt — a no-op turn.
            }
        }
        tokio::task::yield_now().await;
    }
}

/// Queue key: `(table_key, branch_ref)`. `branch_ref = None` means main.
///
/// Branch is part of the key because the same Lance dataset can be
/// pinned at different versions on different branches; concurrent
/// writes to the same `table_key` on disjoint branches must NOT
/// serialize at the queue.
pub(crate) type TableQueueKey = (String, Option<String>);

/// Non-cloneable ownership of the sole immutable export cut for one graph.
///
/// The root registry stores weak references, so retaining the manager here is
/// part of the exclusion contract: a cut can outlive the `Omnigraph` handle
/// that captured it without a reopened handle creating an independent gate.
#[must_use = "dropping the permit releases the export cut"]
pub(crate) struct ExportCutPermit {
    _manager: Arc<WriteQueueManager>,
    _permit: OwnedRwLockWriteGuard<()>,
}

/// Shared exclusion held by controls that can remove or reuse an export cut's
/// exact path/version coordinates.
#[must_use = "dropping the permit releases export destructive control"]
pub(crate) struct ExportDestructivePermit {
    _manager: Arc<WriteQueueManager>,
    _permit: OwnedRwLockReadGuard<()>,
}

/// Per-`(table_key, branch)` writer queue manager.
///
/// Every `Omnigraph` handle for one canonical root identity shares the same
/// manager via a process-global weak registry. This matters beyond HTTP's usual
/// `Arc<Omnigraph>` shape: a separately-opened handle can run recovery, and
/// Lance Restore/ref deletion must serialize with a live writer owned by the
/// first handle. The registry deliberately keys only by the queue root
/// identity; custom storage adapters for the same URI conservatively serialize
/// too.
#[derive(Default)]
pub(crate) struct WriteQueueManager {
    /// Held only briefly per `acquire` call: clone out the per-key Arc,
    /// release the std mutex, then await the per-key tokio Mutex.
    queues: Mutex<HashMap<TableQueueKey, Arc<QueueSlot>>>,
    /// Coarse per-branch effect gate used by sidecar-backed RFC-022 writers.
    ///
    /// This is deliberately separate from `queues`: a branch is authority,
    /// not a synthetic table key. Registered graph-visible effect writers
    /// acquire this gate before any table queue and hold it through manifest
    /// publication; explicit authority/physical exceptions follow their own
    /// registered ordering contracts.
    branch_queues: Mutex<HashMap<Option<String>, Arc<QueueSlot>>>,
    /// One immutable export owns the write side through output. Cooperative
    /// destructive controls share the read side, so they remain mutually
    /// concurrent but cannot remove a path/version beneath a live cut.
    export_gate: Arc<AsyncRwLock<()>>,
}

impl WriteQueueManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return the process-wide queue manager for one canonical graph-root
    /// identity.
    /// Weak values avoid retaining every graph URI ever opened by a long-lived
    /// multi-tenant process; lookup opportunistically removes dead entries.
    pub(crate) fn for_root(root_identity: &str) -> Arc<Self> {
        static REGISTRY: OnceLock<Mutex<HashMap<String, Weak<WriteQueueManager>>>> =
            OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut roots = registry.lock().expect("root write queue registry poisoned");
        if let Some(existing) = roots.get(root_identity).and_then(Weak::upgrade) {
            return existing;
        }
        roots.retain(|_, manager| manager.strong_count() > 0);
        let manager = Arc::new(Self::new());
        roots.insert(root_identity.to_string(), Arc::downgrade(&manager));
        manager
    }

    /// Get-or-create the per-key queue and clone its Arc.
    fn slot(&self, key: &TableQueueKey) -> Arc<QueueSlot> {
        let mut map = self.queues.lock().expect("write queue map poisoned");
        if let Some(existing) = map.get(key) {
            return Arc::clone(existing);
        }
        let fresh = QueueSlot::new();
        map.insert(key.clone(), Arc::clone(&fresh));
        fresh
    }

    fn branch_slot(&self, branch: &Option<String>) -> Arc<QueueSlot> {
        let mut map = self
            .branch_queues
            .lock()
            .expect("branch write queue map poisoned");
        if let Some(existing) = map.get(branch) {
            return Arc::clone(existing);
        }
        let fresh = QueueSlot::new();
        map.insert(branch.clone(), Arc::clone(&fresh));
        fresh
    }

    /// Acquire the coarse effect gate for one graph branch.
    ///
    /// RFC-022-enrolled callers MUST acquire this before any per-table queue.
    /// It is an in-process contention optimization only; publisher OCC and
    /// recovery remain the correctness authorities.
    pub(crate) async fn acquire_branch(&self, branch: Option<&str>) -> QueueGuard {
        let key = branch.map(str::to_string);
        scheduled_lock(self.branch_slot(&key)).await
    }

    /// Reserve the sole immutable export cut without waiting.
    pub(crate) fn try_acquire_export_cut(self: &Arc<Self>) -> Option<ExportCutPermit> {
        let permit = Arc::clone(&self.export_gate).try_write_owned().ok()?;
        Some(ExportCutPermit {
            _manager: Arc::clone(self),
            _permit: permit,
        })
    }

    /// Exclude a live export while destructive control is active.
    ///
    /// Destructive controls take the shared side so unrelated controls keep
    /// their existing concurrency; they only serialize against an export cut.
    pub(crate) fn try_acquire_export_destructive(
        self: &Arc<Self>,
    ) -> Option<ExportDestructivePermit> {
        let permit = Arc::clone(&self.export_gate).try_read_owned().ok()?;
        Some(ExportDestructivePermit {
            _manager: Arc::clone(self),
            _permit: permit,
        })
    }

    /// Acquire several graph-branch control gates in one deterministic order.
    ///
    /// Native branch create-from reads a source ref and mutates a target ref, so
    /// both incarnations must remain stable across its fresh revalidation and
    /// visibility point. Sorting/deduping gives branch control the same
    /// deadlock-free acquisition rule as [`Self::acquire_many`] gives tables.
    pub(crate) async fn acquire_branches(&self, branches: &[Option<String>]) -> Vec<QueueGuard> {
        if branches.is_empty() {
            return Vec::new();
        }
        let mut sorted = branches.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut guards = Vec::with_capacity(sorted.len());
        for branch in sorted {
            guards.push(scheduled_lock(self.branch_slot(&branch)).await);
        }
        guards
    }

    /// Acquire exclusive access to the queue for one `(table_key, branch)`.
    ///
    /// Blocks until the lock is available. Drop the returned guard to
    /// release; the lock outlives the `WriteQueueManager` borrow.
    pub(crate) async fn acquire(&self, key: &TableQueueKey) -> QueueGuard {
        scheduled_lock(self.slot(key)).await
    }

    /// Acquire exclusive access to many `(table_key, branch)` keys
    /// atomically, in lex-sorted order. Used by multi-table writers
    /// (mutation finalize, branch_merge, recovery) so all callers
    /// agree on acquisition order — prevents lock-order inversion.
    ///
    /// Empty input returns an empty Vec without touching the map.
    /// Duplicates in `keys` are deduped before acquisition (the same
    /// key acquired twice would deadlock against itself).
    pub(crate) async fn acquire_many(&self, keys: &[TableQueueKey]) -> Vec<QueueGuard> {
        if keys.is_empty() {
            return Vec::new();
        }
        let mut sorted: Vec<TableQueueKey> = keys.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut guards = Vec::with_capacity(sorted.len());
        for key in &sorted {
            guards.push(self.acquire(key).await);
        }
        guards
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_queue_root_identity;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use tokio::time::timeout;

    fn key(table: &str, branch: Option<&str>) -> TableQueueKey {
        (table.to_string(), branch.map(str::to_string))
    }

    #[tokio::test]
    async fn acquire_many_empty_returns_empty() {
        let qm = WriteQueueManager::new();
        let guards = qm.acquire_many(&[]).await;
        assert!(guards.is_empty());
    }

    #[tokio::test]
    async fn acquire_many_dedupes_repeated_keys() {
        // Same key passed twice would deadlock if not deduped.
        let qm = WriteQueueManager::new();
        let k = key("t1", None);
        let guards = timeout(
            Duration::from_secs(2),
            qm.acquire_many(&[k.clone(), k.clone(), k]),
        )
        .await
        .expect("acquire_many with duplicates deadlocked");
        assert_eq!(guards.len(), 1);
    }

    #[tokio::test]
    async fn acquire_branches_dedupes_main_and_named_keys() {
        let qm = WriteQueueManager::new();
        let guards = timeout(
            Duration::from_secs(2),
            qm.acquire_branches(&[
                Some("feature".to_string()),
                None,
                Some("feature".to_string()),
                None,
            ]),
        )
        .await
        .expect("duplicate branch keys must not self-deadlock");
        assert_eq!(guards.len(), 2);
    }

    #[test]
    fn export_cut_and_destructive_controls_share_one_root_gate() {
        let root = format!("memory://export-gate/{}", ulid::Ulid::new());
        let first = WriteQueueManager::for_root(&root);
        let second = WriteQueueManager::for_root(&root);

        let destructive_a = first
            .try_acquire_export_destructive()
            .expect("first destructive control must acquire");
        let destructive_b = second
            .try_acquire_export_destructive()
            .expect("destructive controls must remain mutually concurrent");
        assert!(second.try_acquire_export_cut().is_none());
        drop(destructive_a);
        drop(destructive_b);

        let cut = first
            .try_acquire_export_cut()
            .expect("export must acquire after destructive controls release");
        assert!(second.try_acquire_export_cut().is_none());
        assert!(second.try_acquire_export_destructive().is_none());
        drop(cut);

        assert!(second.try_acquire_export_cut().is_some());
    }

    #[tokio::test]
    async fn acquire_many_sorts_keys_deterministically() {
        // Two callers passing keys in different orders must acquire in
        // the same internal order. We test this indirectly: caller A
        // passes [a, c] and caller B passes [c, a]; if they both
        // acquire in sorted order the second caller blocks on `a` first,
        // not `c` — same as A — so no deadlock under any interleaving.
        // Direct sort observation: call acquire_many with a reversed
        // input and verify it doesn't deadlock against a held guard on
        // the sorted-first key.
        let qm = Arc::new(WriteQueueManager::new());
        let a = key("a", None);
        let z = key("z", None);

        // Hold `a` exclusively.
        let _held = qm.acquire(&a).await;

        // acquire_many([z, a]) — must sort to [a, z] internally and
        // block on `a`. With a 200ms timeout we should NOT see it
        // complete (it's blocked on `a`).
        let qm2 = Arc::clone(&qm);
        let z_clone = z.clone();
        let a_clone = a.clone();
        let result = timeout(Duration::from_millis(200), async move {
            qm2.acquire_many(&[z_clone, a_clone]).await
        })
        .await;
        assert!(
            result.is_err(),
            "acquire_many should block on `a`, the lex-first key"
        );
    }

    #[tokio::test]
    async fn same_key_acquire_serializes() {
        let qm = Arc::new(WriteQueueManager::new());
        let k = key("t1", None);

        let first = qm.acquire(&k).await;

        // Second acquire on same key should NOT complete within 200ms.
        let qm2 = Arc::clone(&qm);
        let k2 = k.clone();
        let blocked = timeout(
            Duration::from_millis(200),
            async move { qm2.acquire(&k2).await },
        )
        .await;
        assert!(blocked.is_err(), "second acquire on same key must block");

        // Drop the first guard, then second acquire should succeed.
        drop(first);
        let _second = timeout(Duration::from_secs(2), qm.acquire(&k))
            .await
            .expect("second acquire after release should not block");
    }

    #[tokio::test]
    async fn disjoint_keys_acquire_concurrently() {
        let qm = Arc::new(WriteQueueManager::new());
        let a = key("a", None);
        let b = key("b", None);

        // Hold `a` indefinitely.
        let _held_a = qm.acquire(&a).await;

        // Acquire `b` on a different task. Should complete promptly
        // because `b` is disjoint from `a`.
        let qm2 = Arc::clone(&qm);
        let start = Instant::now();
        let _held_b = timeout(Duration::from_secs(2), qm2.acquire(&b))
            .await
            .expect("disjoint key acquire must not block on unrelated held key");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "disjoint acquire took {:?}, should be near-instant",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn disjoint_branches_on_same_table_do_not_serialize() {
        // (table, main) and (table, feature) are different keys.
        let qm = Arc::new(WriteQueueManager::new());
        let main_k = key("t1", None);
        let feature_k = key("t1", Some("feature"));

        let _held_main = qm.acquire(&main_k).await;
        let _held_feature = timeout(Duration::from_secs(2), qm.acquire(&feature_k))
            .await
            .expect("same-table-different-branch should not serialize");
    }

    #[test]
    fn opaque_root_registry_shares_manager_across_handles() {
        let root = format!("memory://write-queue-registry/{}", ulid::Ulid::new());
        let first = WriteQueueManager::for_root(&root);
        let second = WriteQueueManager::for_root(&root);
        assert!(Arc::ptr_eq(&first, &second));

        let other = WriteQueueManager::for_root(&format!("{root}/other"));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn relative_and_absolute_local_roots_share_manager() {
        let relative = PathBuf::from("target")
            .join("write-queue-identities")
            .join(ulid::Ulid::new().to_string())
            .join("graph.omni");
        let absolute = std::env::current_dir().unwrap().join(&relative);
        let relative_identity = write_queue_root_identity(relative.to_str().unwrap()).unwrap();
        let absolute_identity = write_queue_root_identity(absolute.to_str().unwrap()).unwrap();

        assert_eq!(relative_identity, absolute_identity);
        let first = WriteQueueManager::for_root(&relative_identity);
        let second = WriteQueueManager::for_root(&absolute_identity);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[cfg(unix)]
    #[test]
    fn real_and_symlinked_local_roots_share_manager_before_init() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let real_parent = parent.path().join("real");
        let alias_parent = parent.path().join("alias");
        std::fs::create_dir(&real_parent).unwrap();
        symlink(&real_parent, &alias_parent).unwrap();

        // The graph suffix deliberately does not exist: init computes its
        // queue identity before creating the graph directory.
        let real_root = real_parent.join("future").join("graph.omni");
        let alias_root = alias_parent.join("future").join("graph.omni");
        let real_identity = write_queue_root_identity(real_root.to_str().unwrap()).unwrap();
        let alias_identity = write_queue_root_identity(alias_root.to_str().unwrap()).unwrap();

        assert_eq!(real_identity, alias_identity);
        let first = WriteQueueManager::for_root(&real_identity);
        let second = WriteQueueManager::for_root(&alias_identity);
        assert!(Arc::ptr_eq(&first, &second));
    }
}
