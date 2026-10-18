//! Statement-scoped reuse of committed-state reads: one load attempt shares a
//! Lance session and held table handles across its validation opens, so
//! identical metadata, index and open reads are fetched once per statement.

use std::fmt;
use std::sync::{Arc, Weak};

use lance::io::ObjectStoreRegistry;
use omnigraph_core::read_memo::{RangeMemoWrapper, RangeReadMemo};

use crate::db::manifest::SnapshotReadCaches;
use crate::db::{Omnigraph, Snapshot};
use crate::engine::LoadReuseReservation;
use crate::runtime_cache::TableHandleCache;

/// One load attempt's admitted reuse caches and the reservation charging them
/// to `query_memory_bytes`. Opened stores retain the admission if they
/// outlive the attempt.
pub(crate) struct LoadReadReuse {
    caches: SnapshotReadCaches,
    _memo: Arc<RangeReadMemo>,
    _lease: Arc<ReadReuseLease>,
}

/// The actual stores can outlive their initiating load future. Their memo
/// shares this lease, so cached bytes cannot outlive their admission.
struct ReadReuseLease {
    _admitted: LoadReuseReservation,
    registry: Weak<ObjectStoreRegistry>,
}

impl fmt::Debug for ReadReuseLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadReuseLease").finish_non_exhaustive()
    }
}

impl Drop for ReadReuseLease {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            drop(registry.active_stores());
        }
    }
}

impl LoadReadReuse {
    /// Build phase-local caches from an already admitted allowance.
    pub(crate) fn new(
        db: &Omnigraph,
        snapshot: &Snapshot,
        admitted: LoadReuseReservation,
    ) -> Option<Self> {
        let metadata = snapshot
            .clone_allocation_bytes()
            .saturating_add(std::mem::size_of::<Self>())
            .saturating_add(std::mem::size_of::<ReadReuseLease>())
            .saturating_add(128);
        let want = admitted.bytes().checked_sub(metadata)?;
        if want < 1024 * 1024 {
            return None;
        }
        let per_cache = want / 4;
        let registry = db.read_caches().session.store_registry();
        let lease = Arc::new(ReadReuseLease {
            _admitted: admitted,
            registry: Arc::downgrade(&registry),
        });
        let session = crate::lance_access::statement_session(registry, per_cache, per_cache);
        let memo = RangeReadMemo::with_lease((want / 2) as u64, lease.clone());
        let wrapper = RangeMemoWrapper::new(&memo);
        Some(Self {
            caches: SnapshotReadCaches {
                session,
                handles: Arc::new(TableHandleCache::default()),
                table_wrapper: Some(wrapper),
            },
            _memo: memo,
            _lease: lease,
        })
    }

    pub(crate) fn snapshot_caches(&self) -> SnapshotReadCaches {
        self.caches.clone()
    }
}
