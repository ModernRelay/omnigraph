//! Statement-scoped reuse of committed-state reads: one load attempt shares a
//! Lance session and held table handles across its validation opens, so
//! identical metadata, index and open reads are fetched once per statement.

use std::fmt;
use std::sync::{Arc, Weak};

use lance::io::{ObjectStoreRegistry, WrappingObjectStore};
use omnigraph_core::read_memo::{RangeMemoWrapper, RangeReadMemo};

use crate::db::Omnigraph;
use crate::db::manifest::SnapshotReadCaches;
use crate::engine::{LoadMemory, LoadReuseReservation};
use crate::runtime_cache::TableHandleCache;

/// Retained input, validation state and staging charge the same statement
/// pool, so reuse caches take at most this share of it.
const REUSE_ALLOWANCE_DIVISOR: u64 = 4;
const REUSE_CAP_BYTES: u64 = 64 * 1024 * 1024;
const REUSE_FLOOR_BYTES: u64 = 1024 * 1024;

/// One load attempt's admitted reuse caches and the reservation charging them
/// to `query_memory_bytes`. Opened stores retain the admission if they
/// outlive the attempt.
pub(super) struct LoadReadReuse {
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
    /// Reserve the statement's reuse allowance and build the caches it
    /// admits; `None` keeps today's uncached per-open behavior (the fallback
    /// the RFC names for an allowance that cannot admit the buffers).
    pub(super) fn admit(db: &Omnigraph, memory: &LoadMemory) -> Option<Self> {
        let want = (memory.limit() / REUSE_ALLOWANCE_DIVISOR).min(REUSE_CAP_BYTES);
        if want < REUSE_FLOOR_BYTES {
            return None;
        }
        let admitted = memory.admit_read_reuse(want)?;
        let per_cache = usize::try_from(want / 4).ok()?;
        let registry = db.read_caches().session.store_registry();
        let lease = Arc::new(ReadReuseLease {
            _admitted: admitted,
            registry: Arc::downgrade(&registry),
        });
        let session = crate::lance_access::statement_session(registry, per_cache, per_cache);
        let memo = RangeReadMemo::with_lease(want / 2, lease.clone());
        let wrapper: Arc<dyn WrappingObjectStore> = RangeMemoWrapper::new(&memo);
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

    pub(super) fn snapshot_caches(&self) -> SnapshotReadCaches {
        self.caches.clone()
    }
}
