//! Publications whose manifest request got no definite answer.
//!
//! A `__manifest` commit whose manifest put-if-absent stayed ambiguous
//! through every re-issue (`manifest_request`) may still land at the version
//! it attempted. It is ***pending*** on its branch. This process keeps what
//! re-issuing it takes, the base it read and the exact Lance transaction, and
//! the branch refuses new publications until the store answers: a re-issue
//! that lands, or that finds the version held and reads it back as this
//! commit, completes the write; a version another commit holds proves it
//! lost. Either answer is final, because once the version is held no copy of
//! the pending request can land.
//!
//! The registry is process memory over requests this process sent, derived
//! from nothing durable and authoritative for nothing: the manifest stays
//! the graph's only authority. If the process ends first, the pending request
//! is applied by the store or not, and the next publication of that version
//! wins or loses the put-if-absent against it. Bounded by
//! [`MAX_PENDING_PUBLICATIONS`]; a full registry refuses new publications
//! rather than forgetting one that can still land.

use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

use lance::Dataset;
use lance::dataset::CommitBuilder;
use lance::dataset::transaction::Transaction;

use crate::error::OmniError;
use crate::manifest_request::ManifestRequestProbe;
use crate::publisher::{is_retryable_publish_conflict, map_lance_publish_error};

/// How many pending publications one process holds. Each is a branch whose
/// writes are refused, so a store that leaves this many unanswered is
/// failing; past the bound new publications are refused.
pub const MAX_PENDING_PUBLICATIONS: usize = 64;

/// What a publication needs to re-issue its manifest request.
#[derive(Clone)]
pub(crate) struct PendingPublication {
    /// The `__manifest` dataset at the version the commit read.
    pub(crate) base: Dataset,
    /// The exact transaction: a re-issue writes the same transaction file
    /// and a manifest Lance reads back as this commit.
    pub(crate) transaction: Transaction,
    /// The version it attempted, `base` + 1.
    pub(crate) version: u64,
    pub(crate) graph_commit_id: String,
}

impl std::fmt::Debug for PendingPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingPublication")
            .field("uri", &self.base.uri())
            .field("branch", &self.base.manifest().branch)
            .field("version", &self.version)
            .field("graph_commit_id", &self.graph_commit_id)
            .finish()
    }
}

/// The answer one resolution attempt got.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// The attempted version holds the pending commit.
    Committed,
    /// Another commit holds the attempted version.
    Lost,
    /// The store answered nothing definite; the publication stays pending.
    Pending,
}

static PENDING: LazyLock<Mutex<BTreeMap<String, PendingPublication>>> =
    LazyLock::new(Default::default);

/// The registry key of `dataset`'s branch.
pub(crate) fn branch_key(dataset: &Dataset) -> String {
    format!(
        "{}#{}",
        dataset.uri(),
        dataset.manifest().branch.as_deref().unwrap_or("")
    )
}

/// Record a publication whose request may still land. Returns false, and
/// keeps nothing, when the registry is full.
pub(crate) fn register(pending: PendingPublication) -> bool {
    let mut registry = PENDING.lock().unwrap();
    let key = branch_key(&pending.base);
    if registry.len() >= MAX_PENDING_PUBLICATIONS && !registry.contains_key(&key) {
        tracing::error!(
            pending = registry.len(),
            "pending-publication registry is full; refusing new publications in this process"
        );
        return false;
    }
    tracing::error!(
        branch = pending.base.manifest().branch.as_deref().unwrap_or("main"),
        version = pending.version,
        graph_commit_id = %pending.graph_commit_id,
        "publication pending: its manifest request may still land; the branch refuses writes \
         until the store answers"
    );
    registry.insert(key, pending);
    true
}

/// Whether this process can admit another pending publication.
pub(crate) fn full() -> bool {
    PENDING.lock().unwrap().len() >= MAX_PENDING_PUBLICATIONS
}

/// How many publications this process holds pending.
pub fn pending_publications() -> usize {
    PENDING.lock().unwrap().len()
}

/// The pending publication of `dataset`'s branch, if any.
pub(crate) fn pending_on(dataset: &Dataset) -> Option<PendingPublication> {
    PENDING.lock().unwrap().get(&branch_key(dataset)).cloned()
}

/// Settle the pending publication of one branch, given the branch's latest
/// `__manifest` version. A version already past the attempted one is read
/// back; otherwise the identical commit is re-issued once.
pub(crate) async fn resolve(pending: &PendingPublication, latest: u64) -> Resolution {
    let resolution = if latest >= pending.version {
        read_back(pending).await
    } else {
        reissue(pending).await
    };
    if resolution != Resolution::Pending {
        let mut registry = PENDING.lock().unwrap();
        let key = branch_key(&pending.base);
        if registry
            .get(&key)
            .is_some_and(|held| held.graph_commit_id == pending.graph_commit_id)
        {
            registry.remove(&key);
        }
        tracing::info!(
                branch = pending.base.manifest().branch.as_deref().unwrap_or("main"),
            version = pending.version,
            graph_commit_id = %pending.graph_commit_id,
            committed = resolution == Resolution::Committed,
            "pending publication resolved"
        );
    }
    resolution
}

async fn read_back(pending: &PendingPublication) -> Resolution {
    match pending.base.checkout_version(pending.version).await {
        Ok(version) => match crate::state::read_manifest_rows(&version).await {
            Ok(rows) if rows.head.graph_commit_id == pending.graph_commit_id => {
                Resolution::Committed
            }
            Ok(_) => Resolution::Lost,
            Err(_) => Resolution::Pending,
        },
        Err(_) => Resolution::Pending,
    }
}

async fn reissue(pending: &PendingPublication) -> Resolution {
    let handler =
        match crate::lance_clone::probed_commit_handler(&pending.base, ManifestRequestProbe::new())
            .await
        {
            Ok(handler) => handler,
            Err(_) => return Resolution::Pending,
        };
    // forbidden-api-allow: re-issues the exact zero-retry `__manifest` transaction a pending publication attempted; registered in DURABLE_CALLS
    let committed = CommitBuilder::new(Arc::new(pending.base.clone()))
        .with_max_retries(0)
        .with_commit_handler(handler)
        .with_skip_auto_cleanup(true)
        .execute(pending.transaction.clone())
        .await;
    match committed {
        Ok(dataset) if dataset.version().version == pending.version => Resolution::Committed,
        Ok(_) => Resolution::Pending,
        Err(error) => {
            if is_retryable_publish_conflict(&map_lance_publish_error(error)) {
                Resolution::Lost
            } else {
                Resolution::Pending
            }
        }
    }
}

/// Settle every pending publication of this process once: the server's
/// resolver calls this on a backoff, and clean shutdown calls it before it
/// releases admission. Returns how many stay pending.
pub async fn resolve_pending_publications() -> usize {
    let pending: Vec<PendingPublication> = PENDING.lock().unwrap().values().cloned().collect();
    for publication in pending {
        let latest = match publication.base.latest_version_id().await {
            Ok(latest) => latest,
            Err(_) => continue,
        };
        resolve(&publication, latest).await;
    }
    pending_publications()
}

/// The refusal a write to a branch with a pending publication gets: nothing
/// was written, and the caller may retry once the store answers.
pub(crate) fn refusal(pending: &PendingPublication) -> OmniError {
    OmniError::PublicationPending {
        branch: pending.base.manifest().branch.clone(),
        graph_commit_id: Some(pending.graph_commit_id.clone()),
    }
}

/// The refusal every publication gets while the registry is full.
pub(crate) fn registry_full() -> OmniError {
    OmniError::PublicationPending {
        branch: None,
        graph_commit_id: None,
    }
}
