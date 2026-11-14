//! The authority coordinators a handle keeps warm for the branches it uses
//! other than its bound branch: one entry per branch.
//!
//! An entry is the coordinator an operation last captured or published
//! through on that branch: its `__manifest` view, the decoded hot rows behind
//! it and the publisher that retains its last acknowledged image. Writes,
//! merges, branch controls, diffs and named-branch reads start from it, so
//! work on one branch no longer reopens the coordinator because work on
//! another branch ran in between.
//!
//! An entry is a hint, never authority. Every capture probes the branch's
//! latest manifest incarnation before it uses an entry, and every
//! publication still enforces its exact graph-head precondition through the
//! `__manifest` compare-and-swap. An entry belongs to one branch lifetime:
//! a probe that finds the branch deleted, or created again under its name,
//! discards the entry, and the new lifetime is opened from durable state,
//! never refreshed into the old lifetime's entry.
//!
//! The map holds at most [`BRANCH_AUTHORITY_ENTRIES`] branches. Installing a
//! branch beyond that evicts the least recently used one (each lookup and
//! each install counts as a use); an evicted branch is opened again from
//! `__manifest` on its next use. An entry retains its branch's decoded hot
//! `__manifest` rows (table registrations, the commit buffer released at the
//! catalog's history budget, the hot registry rows) and its publisher's
//! image of the last acknowledged publication, which the publisher bounds by
//! its own byte budget. Evictions and discards are reported as `debug`
//! events under the `omnigraph::branch_authority` target.
//!
//! The lock is held only to read or replace an entry, never across a round
//! trip: a lookup hands out an immutable `Arc` view, and a refresh or a
//! publication runs on a copy and installs it. Two operations that miss on
//! the same branch both open it; the later install keeps the newer view.

use std::sync::{Arc, Mutex};

use omnigraph_core::handle_cache::LruMap;

use super::graph_coordinator::GraphCoordinator;

/// The most branches whose coordinator a handle keeps warm. A writer per
/// branch beyond this keeps reopening, as every named-branch writer did with
/// one shared entry.
pub(crate) const BRANCH_AUTHORITY_ENTRIES: usize = 64;

/// Why an entry left the map.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Discard {
    /// The probe failed: the branch is gone or storage refused.
    ProbeFailed,
    /// The branch was deleted and created again under its name.
    LifetimeChanged,
    /// Refreshing the entry to the branch's latest version failed.
    RefreshFailed,
    /// A publication starting from the entry failed.
    PublishFailed,
    /// This handle is deleting the branch.
    BranchDeleted,
}

impl Discard {
    fn as_str(self) -> &'static str {
        match self {
            Self::ProbeFailed => "probe_failed",
            Self::LifetimeChanged => "lifetime_changed",
            Self::RefreshFailed => "refresh_failed",
            Self::PublishFailed => "publish_failed",
            Self::BranchDeleted => "branch_deleted",
        }
    }
}

/// The key of a branch: its logical name, `None` for main.
type BranchKey = Option<String>;

fn key(branch: Option<&str>) -> BranchKey {
    branch.map(str::to_string)
}

fn label(branch: Option<&str>) -> &str {
    branch.unwrap_or("main")
}

fn report_discard(branch: Option<&str>, reason: Discard, entries: usize) {
    tracing::debug!(
        target: "omnigraph::branch_authority",
        branch = label(branch),
        reason = reason.as_str(),
        entries,
        "discarded a branch authority",
    );
}

pub(crate) struct BranchAuthorityCache {
    entries: Mutex<LruMap<BranchKey, Arc<GraphCoordinator>>>, // forbidden-api-allow: a cache held across no round trip, not a gate (write_queue §Gates)
}

impl Default for BranchAuthorityCache {
    fn default() -> Self {
        Self::with_capacity(BRANCH_AUTHORITY_ENTRIES)
    }
}

impl BranchAuthorityCache {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(LruMap::new(capacity)), // forbidden-api-allow: a cache held across no round trip, not a gate (write_queue §Gates)
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, LruMap<BranchKey, Arc<GraphCoordinator>>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// The entry of `branch`, counted as a use.
    pub(crate) fn get(&self, branch: Option<&str>) -> Option<Arc<GraphCoordinator>> {
        self.lock().get(&key(branch)).cloned()
    }

    /// Install `next` as the entry of its branch, unless the entry held now
    /// is newer: `next` replaces an entry it was derived from (`derived_from`,
    /// the entry a refresh or publication copied), an entry it supersedes
    /// within one lifetime (`ManifestIncarnation::supersedes`), or no entry;
    /// it never replaces a newer version of its lifetime, nor a lifetime
    /// another install has already left. Returns the installed view, or
    /// `next` itself when the newer entry stays.
    pub(crate) fn install(
        &self,
        next: GraphCoordinator,
        derived_from: Option<&Arc<GraphCoordinator>>,
    ) -> Arc<GraphCoordinator> {
        let next = Arc::new(next);
        let branch = key(next.current_branch());
        let (evicted, held) = {
            let mut entries = self.lock();
            let replaces = match entries.get(&branch) {
                None => true,
                Some(held) => {
                    derived_from.is_some_and(|derived| Arc::ptr_eq(held, derived))
                        || next
                            .manifest_incarnation()
                            .supersedes(&held.manifest_incarnation())
                }
            };
            if !replaces {
                return next;
            }
            let evicted = entries.insert_evicting(branch, Arc::clone(&next));
            (evicted, entries.len())
        };
        // Report, and drop what left, outside the lock.
        for (branch, _) in evicted {
            tracing::debug!(
                target: "omnigraph::branch_authority",
                branch = label(branch.as_deref()),
                reason = "capacity",
                entries = held,
                "evicted the least recently used branch authority",
            );
        }
        next
    }

    /// Discard the entry of `branch` when it is still `held`, the view the
    /// caller found unusable; a newer entry another operation installed
    /// since stays.
    pub(crate) fn discard(
        &self,
        branch: Option<&str>,
        held: &Arc<GraphCoordinator>,
        reason: Discard,
    ) {
        let branch_key = key(branch);
        let (discarded, entries) = {
            let mut entries = self.lock();
            let still_held = entries
                .peek(&branch_key)
                .is_some_and(|current| Arc::ptr_eq(current, held));
            let discarded = still_held.then(|| entries.remove(&branch_key)).flatten();
            (discarded, entries.len())
        };
        if discarded.is_some() {
            report_discard(branch, reason, entries);
        }
    }

    /// Remove and return the entry of `branch`, which this handle is about
    /// to delete.
    pub(crate) fn take(&self, branch: Option<&str>) -> Option<Arc<GraphCoordinator>> {
        let (taken, entries) = {
            let mut entries = self.lock();
            let taken = entries.remove(&key(branch));
            (taken, entries.len())
        };
        if taken.is_some() {
            report_discard(branch, Discard::BranchDeleted, entries);
        }
        taken
    }

    /// The branches held, least recently used first.
    #[cfg(test)]
    fn branches_by_recency(&self) -> Vec<BranchKey> {
        self.lock().keys_by_recency()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use omnigraph_compiler::settings::SessionSettings;

    use super::*;
    use crate::db::Omnigraph;

    const SCHEMA: &str = "node Person {\n    name: String @key\n}\n";

    async fn graph_with_branches(dir: &tempfile::TempDir, branches: &[&str]) -> Arc<Omnigraph> {
        let db = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        for branch in branches {
            db.branch_create(branch).await.unwrap();
        }
        Arc::new(db)
    }

    fn branch(name: &str) -> BranchKey {
        Some(name.to_string())
    }

    /// Beyond its bound the cache evicts the least recently used branch, a
    /// lookup counting as a use, and main is held like any other branch.
    #[tokio::test]
    async fn the_least_recently_used_branch_leaves_beyond_the_bound() {
        let dir = tempfile::tempdir().unwrap();
        let db = graph_with_branches(&dir, &["a", "b", "c"]).await;
        let cache = BranchAuthorityCache::with_capacity(2);
        for name in ["a", "b"] {
            let opened = db.open_coordinator_for_branch(Some(name)).await.unwrap();
            cache.install(opened, None);
        }
        assert!(cache.get(Some("a")).is_some());

        let opened = db.open_coordinator_for_branch(Some("c")).await.unwrap();
        cache.install(opened, None);
        assert_eq!(cache.branches_by_recency(), [branch("a"), branch("c")]);
        assert!(cache.get(Some("b")).is_none());

        let opened = db.open_coordinator_for_branch(None).await.unwrap();
        cache.install(opened, None);
        assert_eq!(cache.branches_by_recency(), [branch("c"), None]);
    }

    /// An install never replaces a newer view of the branch's lifetime, a
    /// discard removes only the view it names, and a delete takes the entry.
    #[tokio::test]
    async fn installs_keep_the_newest_view_and_discards_name_theirs() {
        let dir = tempfile::tempdir().unwrap();
        let db = graph_with_branches(&dir, &["a"]).await;
        let older = db.open_coordinator_for_branch(Some("a")).await.unwrap();
        crate::Session::from_defaults(Arc::clone(&db), SessionSettings::default())
            .load_with_receipt(
                "a",
                r#"{"type":"Person","data":{"name":"x"}}"#,
                crate::loader::LoadMode::Merge,
            )
            .await
            .unwrap();
        let newer = db.open_coordinator_for_branch(Some("a")).await.unwrap();
        assert!(newer.version() > older.version());

        let cache = BranchAuthorityCache::default();
        let newer = cache.install(newer, None);
        let older = cache.install(older, None);
        let held = cache.get(Some("a")).unwrap();
        assert!(
            Arc::ptr_eq(&held, &newer),
            "an older view never replaces a newer one"
        );

        cache.discard(Some("a"), &older, Discard::PublishFailed);
        assert!(
            cache
                .get(Some("a"))
                .is_some_and(|held| Arc::ptr_eq(&held, &newer))
        );
        let refreshed = cache.install(GraphCoordinator::clone(&newer), Some(&newer));
        assert!(
            cache
                .get(Some("a"))
                .is_some_and(|held| Arc::ptr_eq(&held, &refreshed))
        );
        cache.discard(Some("a"), &newer, Discard::RefreshFailed);
        assert!(
            cache.get(Some("a")).is_some(),
            "a view installed since stays"
        );

        cache.discard(Some("a"), &refreshed, Discard::ProbeFailed);
        assert!(cache.get(Some("a")).is_none());
        cache.install(GraphCoordinator::clone(&newer), None);
        assert!(cache.take(Some("a")).is_some());
        assert!(cache.get(Some("a")).is_none());
    }
}
