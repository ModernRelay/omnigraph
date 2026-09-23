//! The DataFusion session and shared memory pool for one v2 read. Every
//! lowered operator and nested scan executes through its `TaskContext`.
//! Graph breakers refuse reservations beyond the pool's budget; DataFusion
//! sorts and aggregates may spill within the scratch quota.

use std::fmt;
use std::num::NonZero;
use std::sync::{Arc, Mutex};

use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::{
    FairSpillPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation, TrackConsumersPool,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

use super::operators::memory::QueryResources;
use crate::error::{OmniError, Result};
use crate::table_store::{
    ORDERED_SCAN_EXECUTION_BATCH_ROWS, ORDERED_SCAN_MEMORY_BYTES, ORDERED_SCAN_SCRATCH_BYTES,
    sort_spill_reservation_bytes,
};

/// The query pool's size: the ordered scan's constant unless a test installed
/// `instrumentation::with_query_memory_limit` on the task.
pub(super) fn query_memory_limit() -> u64 {
    crate::instrumentation::query_memory_limit().unwrap_or(ORDERED_SCAN_MEMORY_BYTES)
}

pub(super) struct QueryContext {
    session: SessionContext,
    pool: Arc<SpillHeadroomPool>,
    memory_limit: u64,
    scratch_limit: u64,
}

impl QueryContext {
    /// One session per query: `TrackConsumersPool` over `FairSpillPool` so a
    /// refusal names the consumers, one partition so row order is the
    /// operators' own, the ordered scan's batch size, scratch quota and sort
    /// reservation, the pool `memory_limit` sizes: the limit the run's
    /// `QuerySource` captured, never the ambient one.
    pub(super) fn new(memory_limit: u64) -> Result<Self> {
        let scratch_limit = ORDERED_SCAN_SCRATCH_BYTES;
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_batch_size(ORDERED_SCAN_EXECUTION_BATCH_ROWS)
            .with_sort_spill_reservation_bytes(sort_spill_reservation_bytes(memory_limit));
        let pool = Arc::new(SpillHeadroomPool::new(memory_limit as usize));
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(scratch_limit),
            )
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build_arc()
            .map_err(OmniError::datafusion_internal)?;
        Ok(Self {
            session: SessionContext::new_with_config_rt(
                config.with_extension(Arc::new(QueryResources::new(
                    Arc::clone(&runtime.memory_pool),
                    memory_limit,
                ))),
                runtime,
            ),
            pool,
            memory_limit,
            scratch_limit,
        })
    }

    pub(super) fn task_ctx(&self) -> Arc<TaskContext> {
        self.session.task_ctx()
    }

    #[cfg(test)]
    pub(super) fn memory_limit(&self) -> u64 {
        self.memory_limit
    }

    /// A plan failure as the engine's error: a pool or scratch refusal is
    /// the typed resource error, an operator body's `OmniError` comes back
    /// as itself, anything else keeps its DataFusion text.
    pub(super) fn classify(&self, error: DataFusionError) -> OmniError {
        classify_failure(
            error,
            self.pool.as_ref(),
            self.memory_limit,
            self.scratch_limit,
        )
    }
}

/// Both the query boundary and graph operators preserve the pool's refused demand.
pub(super) fn classify_failure(
    error: DataFusionError,
    pool: &dyn MemoryPool,
    memory_limit: u64,
    scratch_limit: u64,
) -> OmniError {
    if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) {
        if crate::table_store::is_scratch_exhaustion(&error) {
            return OmniError::resource_limit(
                "query_scratch_bytes",
                scratch_limit,
                scratch_limit.saturating_add(1),
            );
        }
        let over = memory_limit.saturating_add(1);
        let actual = (pool as &dyn std::any::Any)
            .downcast_ref::<SpillHeadroomPool>()
            .and_then(SpillHeadroomPool::last_refusal)
            .map_or(over, |refused| refused.demanded().max(over));
        return OmniError::resource_limit("query_memory_bytes", memory_limit, actual);
    }
    OmniError::datafusion(error)
}

/// The sizes of one refused reservation: what the pool held and what was asked.
#[derive(Clone, Copy, Debug)]
struct Refusal {
    reserved: usize,
    additional: usize,
}

impl Refusal {
    fn demanded(self) -> u64 {
        (self.reserved as u64).saturating_add(self.additional as u64)
    }
}

/// Cap each spilling reservation at half the query budget and total admission
/// at the full budget. Admission locks before the inner pool's ledger locks.
#[derive(Debug)]
struct SpillHeadroomPool {
    inner: TrackConsumersPool<FairSpillPool>,
    limit: usize,
    admission: Mutex<()>,
    last_refusal: Mutex<Option<Refusal>>,
}

impl SpillHeadroomPool {
    fn new(limit: usize) -> Self {
        Self {
            inner: TrackConsumersPool::new(
                FairSpillPool::new(limit),
                NonZero::new(16).expect("16 is non-zero"),
            ),
            limit,
            admission: Mutex::new(()),
            last_refusal: Mutex::new(None),
        }
    }

    fn last_refusal(&self) -> Option<Refusal> {
        *self
            .last_refusal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn refused<T>(&self, refusal: Refusal, error: DataFusionError) -> DfResult<T> {
        *self
            .last_refusal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(refusal);
        Err(error)
    }
}

impl fmt::Display for SpillHeadroomPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SpillHeadroomPool(spill_limit={}, {})",
            self.limit / 2,
            self.inner
        )
    }
}

impl MemoryPool for SpillHeadroomPool {
    fn name(&self) -> &str {
        "spill_headroom"
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        let _admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.grow(reservation, additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        let _admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DfResult<()> {
        let _admission = self
            .admission
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reserved = self.inner.reserved();
        let refusal = Refusal {
            reserved,
            additional,
        };
        if additional > self.limit.saturating_sub(reserved) {
            return self.refused(
                refusal,
                DataFusionError::ResourcesExhausted(format!(
                    "{} cannot reserve {additional} bytes: {reserved} bytes are reserved, \
                     query limit is {} bytes",
                    reservation.consumer().name(),
                    self.limit,
                )),
            );
        }
        let spill_limit = self.limit / 2;
        let size = reservation.size();
        if reservation.consumer().can_spill() && additional > spill_limit.saturating_sub(size) {
            return self.refused(
                refusal,
                DataFusionError::ResourcesExhausted(format!(
                    "{} cannot reserve {additional} bytes: this reservation holds {size} bytes, \
                     spilling threshold is {spill_limit} bytes",
                    reservation.consumer().name(),
                )),
            );
        }
        match self.inner.try_grow(reservation, additional) {
            Ok(()) => Ok(()),
            Err(error) => self.refused(refusal, error),
        }
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.inner.memory_limit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Compares both error boundaries under one injected pool and disk manager.
    #[test]
    fn graph_and_query_errors_preserve_the_same_limits_and_demand() {
        let pool = Arc::new(SpillHeadroomPool::new(1_024));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .with_disk_manager_builder(
                DiskManagerBuilder::default().with_max_temp_directory_size(1),
            )
            .build_arc()
            .unwrap();
        let config = SessionConfig::new().with_extension(Arc::new(QueryResources::new(
            Arc::clone(&runtime.memory_pool),
            1_024,
        )));
        let query = QueryContext {
            session: SessionContext::new_with_config_rt(config, runtime),
            pool,
            memory_limit: 1_024,
            scratch_limit: 1,
        };
        let work =
            crate::engine::operators::memory::WorkMemory::new(query.task_ctx(), "boundary test")
                .unwrap();
        work.grow(128).unwrap();
        for error in [
            work.error(work.grow(4_096).unwrap_err()),
            query.classify(work.grow(4_096).unwrap_err()),
        ] {
            assert!(
                matches!(error, OmniError::ResourceLimitExceeded { resource, limit: 1_024, actual: 4_224 } if resource == "query_memory_bytes")
            );
        }
        let mut file = query
            .session
            .runtime_env()
            .disk_manager
            .create_tmp_file("boundary scratch")
            .unwrap();
        file.inner().as_file().set_len(2).unwrap();
        for error in [
            work.error(file.update_disk_usage().unwrap_err()),
            query.classify(file.update_disk_usage().unwrap_err()),
        ] {
            assert!(
                matches!(error, OmniError::ResourceLimitExceeded { resource, limit: 1, actual: 2 } if resource == "query_scratch_bytes")
            );
        }
    }

    #[test]
    fn issue_723_spilling_leaves_usable_producer_headroom_under_the_same_cap() {
        let pool: Arc<dyn MemoryPool> = Arc::new(SpillHeadroomPool::new(1_024));
        let merge = MemoryConsumer::new("sort merge").register(&pool);
        let sort = MemoryConsumer::new("sort")
            .with_can_spill(true)
            .register(&pool);
        let producer = MemoryConsumer::new("graph take output").register(&pool);
        merge.try_grow(128).unwrap();
        sort.try_grow(512).unwrap();
        assert!(matches!(
            sort.try_grow(1),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(sort.size(), 512);
        assert_eq!(pool.reserved(), 640);

        producer.try_grow(144).unwrap();
        assert_eq!(pool.reserved(), 784);
        let aggregate = MemoryConsumer::new("aggregate")
            .with_can_spill(true)
            .register(&pool);
        aggregate.try_grow(16).unwrap();
        assert_eq!(pool.reserved(), 800);
        producer.try_grow(224).unwrap();
        assert_eq!(pool.reserved(), 1_024);
        for reservation in [&producer, &aggregate] {
            assert!(matches!(
                reservation.try_grow(1),
                Err(DataFusionError::ResourcesExhausted(_))
            ));
        }
        assert_eq!(producer.size(), 368);
        assert_eq!(aggregate.size(), 16);
        assert_eq!(pool.reserved(), 1_024);
        assert!(matches!(pool.memory_limit(), MemoryLimit::Finite(1_024)));

        drop(aggregate);
        assert_eq!(sort.free(), 512);
        producer.try_grow(528).unwrap();
        assert_eq!(pool.reserved(), 1_024);
        drop(producer);
        drop(merge);
        assert_eq!(pool.reserved(), 0);
        sort.try_grow(512).unwrap();
        drop(sort);
        assert_eq!(pool.reserved(), 0);
    }

    /// A real DataFusion scratch refusal; GQT cannot configure its disk manager.
    #[test]
    fn scratch_exhaustion_uses_the_datafusion_resource_boundary() {
        let disk = Arc::new(
            DiskManagerBuilder::default()
                .with_max_temp_directory_size(1)
                .build()
                .unwrap(),
        );
        let mut file = disk.create_tmp_file("scratch classification").unwrap();
        file.inner().as_file().set_len(2).unwrap();
        let error = file.update_disk_usage().unwrap_err();
        assert!(crate::table_store::is_scratch_exhaustion(&error));
        assert!(!crate::table_store::is_scratch_exhaustion(
            &DataFusionError::ResourcesExhausted("memory pool".into()),
        ));
    }

    #[test]
    fn issue_723_split_spilling_reservations_share_the_query_cap() {
        let pool: Arc<dyn MemoryPool> = Arc::new(SpillHeadroomPool::new(1_024));
        let sort = MemoryConsumer::new("sort")
            .with_can_spill(true)
            .register(&pool);
        sort.try_grow(512).unwrap();
        let first = sort.split(256);
        let second = sort.split(256);
        first.try_grow(256).unwrap();
        second.try_grow(256).unwrap();
        assert_eq!(pool.reserved(), 1_024);
        assert!(matches!(
            sort.try_grow(1),
            Err(DataFusionError::ResourcesExhausted(_))
        ));
        assert_eq!(sort.size(), 0);
        assert_eq!(pool.reserved(), 1_024);
        drop(first);
        sort.try_grow(512).unwrap();
        assert_eq!(pool.reserved(), 1_024);
        drop(second);
        drop(sort);
        assert_eq!(pool.reserved(), 0);
    }
}
