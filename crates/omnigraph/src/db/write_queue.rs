//! Per-`(table_key, branch)` writer queues — MR-686 scaffolding.
//!
//! Today every server-layer write serializes on the global
//! `Arc<RwLock<Omnigraph>>` in `AppState`. MR-686 replaces that with
//! per-`(table_key, branch_ref)` queues so disjoint-key writes proceed
//! concurrently. This module owns the queue data structure; callers in
//! `MutationStaging::commit_all`, `branch_merge`, `schema_apply`,
//! `ensure_indices`, `delete_where`, and the future MR-870 recovery
//! reconciler acquire guards before any per-table Lance commit.
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
//! lexicographic order. Multi-table writers (mutation finalize,
//! branch_merge, future recovery reconciler) MUST go through
//! `acquire_many` so all callers agree on acquisition order — this is
//! how lock-order inversion deadlock is prevented.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

/// Queue key: `(table_key, branch_ref)`. `branch_ref = None` means main.
///
/// Branch is part of the key because the same Lance dataset can be
/// pinned at different versions on different branches; concurrent
/// writes to the same `table_key` on disjoint branches must NOT
/// serialize at the queue.
pub(crate) type TableQueueKey = (String, Option<String>);

/// Per-`(table_key, branch)` writer queue manager.
///
/// Lives on `Omnigraph` as `Arc<WriteQueueManager>` so HTTP handlers,
/// engine internals, the CLI binary, and future background reconcilers
/// (MR-870 recovery, MR-848 index) all reach it via the engine handle.
#[derive(Default)]
pub(crate) struct WriteQueueManager {
    /// Held only briefly per `acquire` call: clone out the per-key Arc,
    /// release the std mutex, then await the per-key tokio Mutex.
    queues: Mutex<HashMap<TableQueueKey, Arc<AsyncMutex<()>>>>,
}

impl WriteQueueManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Get-or-create the per-key queue and clone its Arc.
    fn slot(&self, key: &TableQueueKey) -> Arc<AsyncMutex<()>> {
        let mut map = self.queues.lock().expect("write queue map poisoned");
        if let Some(existing) = map.get(key) {
            return Arc::clone(existing);
        }
        let fresh = Arc::new(AsyncMutex::new(()));
        map.insert(key.clone(), Arc::clone(&fresh));
        fresh
    }

    /// Acquire exclusive access to the queue for one `(table_key, branch)`.
    ///
    /// Blocks until the lock is available. Drop the returned guard to
    /// release; the lock outlives the `WriteQueueManager` borrow.
    pub(crate) async fn acquire(&self, key: &TableQueueKey) -> OwnedMutexGuard<()> {
        self.slot(key).lock_owned().await
    }

    /// Acquire exclusive access to many `(table_key, branch)` keys
    /// atomically, in lex-sorted order. Used by multi-table writers
    /// (mutation finalize, branch_merge, recovery) so all callers
    /// agree on acquisition order — prevents lock-order inversion.
    ///
    /// Empty input returns an empty Vec without touching the map.
    /// Duplicates in `keys` are deduped before acquisition (the same
    /// key acquired twice would deadlock against itself).
    pub(crate) async fn acquire_many(
        &self,
        keys: &[TableQueueKey],
    ) -> Vec<OwnedMutexGuard<()>> {
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
        assert!(result.is_err(), "acquire_many should block on `a`, the lex-first key");
    }

    #[tokio::test]
    async fn same_key_acquire_serializes() {
        let qm = Arc::new(WriteQueueManager::new());
        let k = key("t1", None);

        let first = qm.acquire(&k).await;

        // Second acquire on same key should NOT complete within 200ms.
        let qm2 = Arc::clone(&qm);
        let k2 = k.clone();
        let blocked = timeout(Duration::from_millis(200), async move {
            qm2.acquire(&k2).await
        })
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
}
