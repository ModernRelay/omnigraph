//! Private ownership for speculative table preparation.
//!
//! These accounts follow controlled allocations, not task lifetimes. A finished
//! worker can still retain data behind an earlier ordered result, and its leases
//! remain charged until collection explicitly transfers the worker's ownership.
//! Reservations never wait for another worker to release memory.

use std::future::Future;
use std::path::Path;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tempfile::TempDir;

use crate::error::{OmniError, Result};

/// This private signal must be consumed by the preparation scheduler. Existing
/// serial resource refusals must never be interpreted as scheduling pressure.
const PRESSURE_RESOURCE: &str = "branch-merge private parallel preparation pressure";

/// An error closes admission past its ordered slot, while earlier workers must
/// finish far enough to determine the serial error. Every replay owns a fresh
/// frontier. The private stop outcome follows the pressure/fallback path only
/// when an earlier pressure outcome actually requires replay.
#[derive(Debug)]
pub(super) struct PreparationFailureFrontier {
    first_failed_slot: AtomicUsize,
}

impl PreparationFailureFrontier {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            first_failed_slot: AtomicUsize::new(usize::MAX),
        })
    }

    pub(super) fn failed(&self, slot: usize) {
        self.first_failed_slot.fetch_min(slot, Ordering::Relaxed);
    }

    fn checkpoint(&self, slot: usize) -> Result<()> {
        if slot > self.first_failed_slot.load(Ordering::Relaxed) {
            Err(OmniError::resource_limit(
                format!("{PRESSURE_RESOURCE}: stopped after an earlier table failure"),
                0,
                1,
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub(super) struct PreparationBudget {
    limit: u64,
    retained_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    reservation_failures: AtomicU64,
    reporter: crate::instrumentation::MergePreparationReporter,
    scratch: Arc<ScratchAccounting>,
}

/// Counters are independently sampled. Inspect after settlement for a final
/// reading; live samples are gauges rather than a transactionally frozen view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PreparationBudgetSnapshot {
    pub(super) retained_bytes: u64,
    pub(super) peak_bytes: u64,
    pub(super) reservation_failures: u64,
}

impl PreparationBudget {
    pub(super) fn new(limit: u64) -> Arc<Self> {
        let reporter = crate::instrumentation::merge_preparation_reporter();
        Arc::new(Self {
            limit,
            retained_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            reservation_failures: AtomicU64::new(0),
            scratch: Arc::new(ScratchAccounting::new(reporter.clone())),
            reporter,
        })
    }

    pub(super) fn snapshot(&self) -> PreparationBudgetSnapshot {
        PreparationBudgetSnapshot {
            retained_bytes: self.retained_bytes.load(Ordering::Relaxed),
            peak_bytes: self.peak_bytes.load(Ordering::Relaxed),
            reservation_failures: self.reservation_failures.load(Ordering::Relaxed),
        }
    }

    fn acquire(&self, bytes: u64) -> std::result::Result<(), Pressure> {
        let mut retained = self.retained_bytes.load(Ordering::Relaxed);
        loop {
            let next = retained.checked_add(bytes);
            if next.is_none_or(|next| next > self.limit) {
                self.reservation_failures.fetch_add(1, Ordering::Relaxed);
                return Err(Pressure {
                    limit: self.limit,
                    actual: next.unwrap_or(u64::MAX),
                });
            }
            let next = next.expect("checked preparation reservation has a total");
            match self.retained_bytes.compare_exchange_weak(
                retained,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak_bytes.fetch_max(next, Ordering::Relaxed);
                    self.reporter
                        .bytes(usize::try_from(next).unwrap_or(usize::MAX));
                    return Ok(());
                }
                Err(observed) => retained = observed,
            }
        }
    }

    fn release(&self, bytes: u64) {
        let previous = self
            .retained_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |retained| {
                retained.checked_sub(bytes)
            })
            .expect("preparation leases release only their owned bytes");
        self.reporter
            .bytes(usize::try_from(previous - bytes).unwrap_or(usize::MAX));
    }
}

#[derive(Debug, Clone, Copy)]
struct Pressure {
    limit: u64,
    actual: u64,
}

impl Pressure {
    fn error(self) -> OmniError {
        OmniError::resource_limit(PRESSURE_RESOURCE, self.limit, self.actual)
    }
}

#[derive(Debug)]
struct WorkerAccounting {
    budget: Option<Arc<PreparationBudget>>,
    retained_bytes: u64,
    pressure: Option<Pressure>,
    scratch: Option<Arc<ScratchAccounting>>,
    scratch_owners: u64,
    scratch_bytes: u64,
}

/// Sum scratch owned by active and ready workers, including serial fallback.
/// Bytes are successfully staged logical Arrow payload, not physical file size,
/// resident memory, or abandoned directories after a forced write-future drop.
#[derive(Debug)]
struct ScratchAccounting {
    totals: Mutex<(u64, u64)>,
    reporter: crate::instrumentation::MergePreparationReporter,
}

impl ScratchAccounting {
    fn new(reporter: crate::instrumentation::MergePreparationReporter) -> Self {
        Self {
            totals: Mutex::new((0, 0)),
            reporter,
        }
    }

    fn update(&self, owners: u64, bytes: u64, add: bool) {
        let mut totals = self
            .totals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if add {
            totals.0 = totals
                .0
                .checked_add(owners)
                .expect("scratch owner count fits u64");
            totals.1 = totals
                .1
                .checked_add(bytes)
                .expect("bounded staged payload fits u64");
        } else {
            totals.0 = totals
                .0
                .checked_sub(owners)
                .expect("release only owned scratch directories");
            totals.1 = totals
                .1
                .checked_sub(bytes)
                .expect("release only owned staged payload");
        }
        self.reporter.scratch(
            usize::try_from(totals.0).unwrap_or(usize::MAX),
            usize::try_from(totals.1).unwrap_or(usize::MAX),
        );
    }
}

/// One worker's account. The short mutex protects transfer against surviving
/// leases; it is never held across an await or used to wait for memory permits.
#[derive(Debug)]
pub(super) struct PreparationContext {
    // Immutable construction flag: serial windows contain one worker, so they
    // have neither parallel-budget pressure nor a sibling failure to observe.
    // Scratch accounting remains active. For parallel-born contexts, the
    // mutable account still governs ownership transfer to the collector.
    parallel_accounting: bool,
    accounting: Mutex<WorkerAccounting>,
    ordered_failure: Option<(Arc<PreparationFailureFrontier>, usize)>,
}

impl PreparationContext {
    #[cfg(test)]
    pub(super) fn new(budget: Option<Arc<PreparationBudget>>) -> Arc<Self> {
        let scratch = budget.as_ref().map_or_else(
            || {
                Arc::new(ScratchAccounting::new(
                    crate::instrumentation::merge_preparation_reporter(),
                ))
            },
            |budget| Arc::clone(&budget.scratch),
        );
        Self::with_accounts(budget, scratch, None)
    }

    /// Serial workers share operation scratch gauges but retain their existing
    /// memory limits instead of the additional speculative memory allowance.
    #[cfg(test)]
    pub(super) fn for_window(budget: Arc<PreparationBudget>, parallel: bool) -> Arc<Self> {
        let scratch = Arc::clone(&budget.scratch);
        Self::with_accounts(parallel.then_some(budget), scratch, None)
    }

    pub(super) fn for_ordered_window(
        budget: Arc<PreparationBudget>,
        parallel: bool,
        frontier: Arc<PreparationFailureFrontier>,
        slot: usize,
    ) -> Arc<Self> {
        let scratch = Arc::clone(&budget.scratch);
        Self::with_accounts(parallel.then_some(budget), scratch, Some((frontier, slot)))
    }

    fn with_accounts(
        budget: Option<Arc<PreparationBudget>>,
        scratch: Arc<ScratchAccounting>,
        ordered_failure: Option<(Arc<PreparationFailureFrontier>, usize)>,
    ) -> Arc<Self> {
        Arc::new(Self {
            parallel_accounting: budget.is_some(),
            accounting: Mutex::new(WorkerAccounting {
                budget,
                retained_bytes: 0,
                pressure: None,
                scratch: Some(scratch),
                scratch_owners: 0,
                scratch_bytes: 0,
            }),
            ordered_failure,
        })
    }

    fn accounting(&self) -> MutexGuard<'_, WorkerAccounting> {
        self.accounting
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn reserve(self: &Arc<Self>, bytes: u64) -> Result<PreparationLease> {
        if !self.parallel_accounting {
            return Ok(PreparationLease {
                context: None,
                bytes,
            });
        }
        self.resize(0, bytes)?;
        Ok(PreparationLease {
            context: Some(Arc::clone(self)),
            bytes,
        })
    }

    fn resize(&self, previous: u64, bytes: u64) -> Result<()> {
        if !self.parallel_accounting {
            return Ok(());
        }
        let mut accounting = self.accounting();
        // Releasing an allocation must stay infallible after a sibling fails.
        // New controlled growth stops before allocating another chunk. Once
        // transferred, its lease follows the collector's serial contract and
        // cannot inherit a subsequently failed speculative window.
        if accounting.scratch.is_some() && bytes > previous {
            self.check_ordered_failure()?;
        }
        let Some(budget) = accounting.budget.as_ref() else {
            return Ok(());
        };
        if bytes <= previous {
            let released = previous - bytes;
            budget.release(released);
            accounting.retained_bytes -= released;
            return Ok(());
        }
        if let Some(pressure) = accounting.pressure {
            return Err(pressure.error());
        }
        let growth = bytes - previous;
        if let Err(pressure) = budget.acquire(growth) {
            accounting.pressure = Some(pressure);
            return Err(pressure.error());
        }
        // A worker is a subset of the successfully charged shared account, so
        // shared-account overflow checking also proves this addition fits.
        accounting.retained_bytes += growth;
        Ok(())
    }

    fn checkpoint(&self) -> Result<()> {
        if !self.parallel_accounting {
            return Ok(());
        }
        let accounting = self.accounting();
        if accounting.scratch.is_some() {
            self.check_ordered_failure()?;
        }
        match accounting.pressure {
            Some(pressure) => Err(pressure.error()),
            None => Ok(()),
        }
    }

    fn check_ordered_failure(&self) -> Result<()> {
        match &self.ordered_failure {
            Some((frontier, slot)) => frontier.checkpoint(*slot),
            None => Ok(()),
        }
    }

    fn require_serial(&self) -> Result<()> {
        if !self.parallel_accounting {
            return Ok(());
        }
        let mut accounting = self.accounting();
        let Some(budget) = accounting.budget.as_ref() else {
            return Ok(());
        };
        if let Some(pressure) = accounting.pressure {
            return Err(pressure.error());
        }
        // This path has no qualified retained-allocation estimate yet. Request
        // serial execution rather than pretending its unobserved bytes are zero.
        let pressure = Pressure {
            limit: budget.limit,
            actual: u64::MAX,
        };
        budget.reservation_failures.fetch_add(1, Ordering::Relaxed);
        accounting.pressure = Some(pressure);
        Err(pressure.error())
    }

    fn scratch_added(&self, owners: u64, bytes: u64) {
        let mut accounting = self.accounting();
        if let Some(scratch) = &accounting.scratch {
            scratch.update(owners, bytes, true);
            accounting.scratch_owners += owners;
            accounting.scratch_bytes += bytes;
        }
    }

    fn scratch_released(&self, bytes: u64) {
        let mut accounting = self.accounting();
        if let Some(scratch) = &accounting.scratch {
            scratch.update(1, bytes, false);
            accounting.scratch_owners -= 1;
            accounting.scratch_bytes -= bytes;
        }
    }

    /// The collector now owns this worker's retained candidate under the
    /// existing serial limits. Surviving leases become untracked, including
    /// future resizes, so neither collection nor later Drop can double-release.
    pub(super) fn release_to_collector(&self) {
        let mut accounting = self.accounting();
        if let Some(budget) = accounting.budget.take() {
            budget.release(accounting.retained_bytes);
            accounting.retained_bytes = 0;
        }
        if let Some(scratch) = accounting.scratch.take() {
            scratch.update(accounting.scratch_owners, accounting.scratch_bytes, false);
            accounting.scratch_owners = 0;
            accounting.scratch_bytes = 0;
        }
        accounting.pressure = None;
    }
}

impl Drop for PreparationContext {
    fn drop(&mut self) {
        // Leases hold an Arc to this context, so the last context drop happens
        // only after their charges were released. The remaining bytes belong
        // to conservative metadata retention and must not leak into a retry.
        self.release_to_collector();
    }
}

tokio::task_local! {
    static PREPARATION_CONTEXT: Arc<PreparationContext>;
}

/// Scope each worker future independently, even when several are polled by the
/// same parent task. Tokio restores the previous scope after every child poll.
pub(super) async fn scope<F: Future>(context: Arc<PreparationContext>, future: F) -> F::Output {
    PREPARATION_CONTEXT.scope(context, future).await
}

pub(super) fn reserve(bytes: u64) -> Result<PreparationLease> {
    PREPARATION_CONTEXT
        .try_with(|context| context.reserve(bytes))
        .unwrap_or_else(|_| {
            Ok(PreparationLease {
                context: None,
                bytes,
            })
        })
}

/// Retain a conservative charge for small metadata until this worker transfers
/// to the collector or its final context reference drops. Use a lease for
/// recyclable batch buffers; this monotonic account intentionally does not
/// recover bytes when a key, conflict string, log entry, or result is replaced.
pub(super) fn retain(bytes: u64) -> Result<()> {
    PREPARATION_CONTEXT
        .try_with(|context| context.resize(0, bytes))
        .unwrap_or(Ok(()))
}

/// A failed worker and slots ordered after any failed sibling stop at bounded
/// checkpoints. Earlier slots continue to preserve serial error ordering. This
/// private stop/pressure signal never drops an already-issued scratch write.
pub(super) fn checkpoint() -> Result<()> {
    PREPARATION_CONTEXT
        .try_with(|context| context.checkpoint())
        .unwrap_or(Ok(()))
}

pub(super) fn parallel_context_active() -> bool {
    PREPARATION_CONTEXT
        .try_with(|context| context.parallel_accounting && context.accounting().budget.is_some())
        .unwrap_or(false)
}

/// Gate a preparation path whose internal allocations are not yet accounted.
/// The scheduler consumes this signal and retries under the serial contract.
pub(super) fn require_serial() -> Result<()> {
    PREPARATION_CONTEXT
        .try_with(|context| context.require_serial())
        .unwrap_or(Ok(()))
}

pub(super) fn is_pressure(error: &OmniError) -> bool {
    match error {
        OmniError::ResourceLimitExceeded { resource, .. } => resource
            .strip_prefix(PRESSURE_RESOURCE)
            .is_some_and(|suffix| {
                suffix.is_empty() || suffix.starts_with(" for ") || suffix.starts_with(": ")
            }),
        _ => false,
    }
}

/// An allocation's charge. Share `Arc<PreparationLease>` when Arrow clones or
/// slices retain the same buffers; cloning the lease itself would double-own
/// the charge. A default lease remains untracked even inside a later scope.
#[derive(Debug, Default)]
pub(super) struct PreparationLease {
    context: Option<Arc<PreparationContext>>,
    bytes: u64,
}

impl PreparationLease {
    /// Untracked serial batches need no shared lease allocation or row clones.
    pub(super) fn into_shared(self) -> Option<Arc<Self>> {
        self.context.is_some().then(|| Arc::new(self))
    }

    pub(super) fn resize(&mut self, bytes: u64) -> Result<()> {
        if let Some(context) = &self.context {
            context.resize(self.bytes, bytes)?;
        }
        self.bytes = bytes;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn bytes(&self) -> u64 {
        self.bytes
    }
}

impl Drop for PreparationLease {
    fn drop(&mut self) {
        if let Some(context) = &self.context {
            // Shrinking cannot fail, including after a pressure event.
            context
                .resize(self.bytes, 0)
                .expect("releasing preparation ownership cannot need more memory");
        }
    }
}

/// A scratch directory whose issued writer may outlive a forcibly dropped
/// operation through underlying filesystem work. Normal paths await the write,
/// call `end_write` even on an error, then permit ordinary TempDir cleanup.
/// Forced drop during the await keeps the private directory instead of racing
/// its deletion against I/O. This does not create a detached cleanup task.
#[derive(Debug)]
pub(super) struct ScratchDirectory {
    dir: Option<TempDir>,
    write_in_flight: bool,
    context: Option<Arc<PreparationContext>>,
    staged_bytes: u64,
}

impl ScratchDirectory {
    pub(super) fn new(dir: TempDir) -> Self {
        let context = PREPARATION_CONTEXT.try_with(Arc::clone).ok();
        if let Some(context) = &context {
            context.scratch_added(1, 0);
        }
        Self {
            dir: Some(dir),
            write_in_flight: false,
            context,
            staged_bytes: 0,
        }
    }

    pub(super) fn path(&self) -> &Path {
        self.dir
            .as_ref()
            .expect("scratch directory remains owned until transfer")
            .path()
    }

    pub(super) fn begin_write(&mut self) {
        assert!(
            !self.write_in_flight,
            "scratch writes for one staging owner must remain serial"
        );
        self.write_in_flight = true;
    }

    pub(super) fn end_write(&mut self) {
        assert!(self.write_in_flight, "scratch write was not started");
        self.write_in_flight = false;
    }

    /// Account successful staging's logical Arrow payload exactly once. Keep
    /// this owner inside the resulting StagedTable until collection or discard.
    /// This diagnostic does not measure disk bytes or enforce the memory budget.
    pub(super) fn add_bytes(&mut self, bytes: u64) {
        let next = self
            .staged_bytes
            .checked_add(bytes)
            .expect("bounded staged payload fits u64");
        if let Some(context) = &self.context {
            context.scratch_added(0, bytes);
        }
        self.staged_bytes = next;
    }
}

impl Drop for ScratchDirectory {
    fn drop(&mut self) {
        if let Some(context) = &self.context {
            context.scratch_released(self.staged_bytes);
        }
        if self.write_in_flight
            && let Some(dir) = self.dir.take()
        {
            let path = dir.keep();
            tracing::warn!(
                path = %path.display(),
                "branch merge scratch write dropped before completion; retaining private directory"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordered_failure_stops_later_growth_but_preserves_earlier_errors_and_cleanup() {
        let budget = PreparationBudget::new(20);
        let frontier = PreparationFailureFrontier::new();
        let earlier = PreparationContext::for_ordered_window(
            Arc::clone(&budget),
            true,
            Arc::clone(&frontier),
            0,
        );
        let later = PreparationContext::for_ordered_window(
            Arc::clone(&budget),
            true,
            Arc::clone(&frontier),
            2,
        );
        let (mut lease, mut scratch) = scope(Arc::clone(&later), async {
            let lease = reserve(5).unwrap();
            let mut scratch = ScratchDirectory::new(tempfile::tempdir().unwrap());
            scratch.begin_write();
            (lease, scratch)
        })
        .await;
        let scratch_path = scratch.path().to_path_buf();

        frontier.failed(1);
        let error = scope(Arc::clone(&earlier), async {
            // A preceding table must still run: its real error outranks slot 1.
            checkpoint().unwrap();
            let _lease = reserve(3).unwrap();
            OmniError::manifest_internal("earlier table failure")
        })
        .await;
        assert!(!is_pressure(&error));
        assert!(is_pressure(
            &scope(Arc::clone(&later), async { checkpoint().unwrap_err() }).await
        ));
        assert!(is_pressure(&lease.resize(6).unwrap_err()));
        assert_eq!(lease.bytes(), 5, "failed growth keeps its previous owner");

        // Already-issued scratch I/O settles normally, even after the cutoff.
        std::fs::write(scratch.path().join("settled"), b"private").unwrap();
        scratch.end_write();
        lease.resize(2).unwrap();
        drop(lease);
        drop(scratch);
        assert!(
            !scratch_path.exists(),
            "settled private scratch is reclaimable"
        );
        assert_eq!(budget.snapshot().retained_bytes, 0);

        // A newly observed earlier error moves the frontier only leftward.
        frontier.failed(0);
        frontier.failed(3);
        scope(earlier, async { checkpoint().unwrap() }).await;
        assert!(is_pressure(
            &scope(later, async { checkpoint().unwrap_err() }).await
        ));
    }

    #[tokio::test]
    async fn replay_frontier_does_not_cancel_replayed_earlier_slots() {
        let budget = PreparationBudget::new(10);
        let old = PreparationFailureFrontier::new();
        old.failed(0);
        let old_worker = PreparationContext::for_ordered_window(Arc::clone(&budget), true, old, 1);
        assert!(is_pressure(
            &scope(old_worker, async { reserve(1).unwrap_err() }).await
        ));

        let replay = PreparationFailureFrontier::new();
        let first = PreparationContext::for_ordered_window(
            Arc::clone(&budget),
            true,
            Arc::clone(&replay),
            0,
        );
        let later = PreparationContext::for_ordered_window(
            Arc::clone(&budget),
            true,
            Arc::clone(&replay),
            2,
        );
        // A carried real error in slot 1 cuts off only subsequent work.
        replay.failed(1);
        scope(first, async {
            checkpoint().unwrap();
            let _lease = reserve(4).unwrap();
        })
        .await;
        assert!(is_pressure(
            &scope(later, async { checkpoint().unwrap_err() }).await
        ));
        assert_eq!(budget.snapshot().retained_bytes, 0);
    }

    #[tokio::test]
    async fn retained_metadata_obeys_shared_cap_and_transfers_once() {
        let budget = PreparationBudget::new(10);
        let first = PreparationContext::new(Some(Arc::clone(&budget)));
        let second = PreparationContext::new(Some(Arc::clone(&budget)));
        scope(Arc::clone(&first), async {
            retain(6).unwrap();
        })
        .await;
        scope(second, async {
            assert!(is_pressure(&retain(5).unwrap_err()));
        })
        .await;
        assert_eq!(budget.snapshot().retained_bytes, 6);
        first.release_to_collector();
        first.release_to_collector();
        scope(first, async {
            retain(u64::MAX).unwrap();
        })
        .await;
        assert_eq!(budget.snapshot().retained_bytes, 0);
        assert_eq!(budget.snapshot().peak_bytes, 6);
        assert_eq!(budget.snapshot().reservation_failures, 1);
    }

    #[tokio::test]
    async fn final_context_drop_releases_residual_metadata_and_updates_gauge() {
        let probes = crate::instrumentation::MergeWriteProbes::default();
        let budget = crate::instrumentation::with_merge_write_probes(probes.clone(), async {
            let budget = PreparationBudget::new(20);
            let context = PreparationContext::new(Some(Arc::clone(&budget)));
            let lease = scope(Arc::clone(&context), async {
                retain(3).unwrap();
                retain(4).unwrap();
                reserve(5).unwrap()
            })
            .await;
            assert_eq!(budget.snapshot().retained_bytes, 12);
            assert_eq!(probes.merge_preparation_snapshot().accounted_bytes, 12);
            drop(context);
            // The lease still owns the context and therefore its metadata.
            assert_eq!(budget.snapshot().retained_bytes, 12);
            drop(lease);
            assert_eq!(budget.snapshot().retained_bytes, 0);
            assert_eq!(probes.merge_preparation_snapshot().accounted_bytes, 0);
            budget
        })
        .await;
        assert_eq!(budget.snapshot().peak_bytes, 12);
        assert_eq!(probes.merge_preparation_snapshot().peak_accounted_bytes, 12);
    }

    #[tokio::test]
    async fn retained_leases_share_one_cap_and_preserve_failed_resize() {
        let budget = PreparationBudget::new(10);
        let first = PreparationContext::new(Some(Arc::clone(&budget)));
        let second = PreparationContext::new(Some(Arc::clone(&budget)));
        let mut first_lease = scope(first, async { reserve(6).unwrap() }).await;
        let second_lease = scope(Arc::clone(&second), async { reserve(4).unwrap() }).await;

        let error = scope(second, async { reserve(1).unwrap_err() }).await;
        assert!(is_pressure(&error));
        assert_eq!(budget.snapshot().retained_bytes, 10);
        assert!(is_pressure(&first_lease.resize(7).unwrap_err()));
        assert_eq!(first_lease.bytes(), 6);
        first_lease.resize(2).unwrap();
        assert_eq!(budget.snapshot().retained_bytes, 6);
        drop(second_lease);
        assert_eq!(budget.snapshot().retained_bytes, 2);
        drop(first_lease);
        assert_eq!(
            budget.snapshot(),
            PreparationBudgetSnapshot {
                retained_bytes: 0,
                peak_bytes: 10,
                reservation_failures: 2,
            }
        );
    }

    #[tokio::test]
    async fn completed_result_and_cloned_rows_hold_charge_until_collection() {
        let budget = PreparationBudget::new(10);
        let frontier = PreparationFailureFrontier::new();
        let context = PreparationContext::for_ordered_window(
            Arc::clone(&budget),
            true,
            Arc::clone(&frontier),
            1,
        );
        let lease = scope(Arc::clone(&context), async {
            Arc::new(reserve(8).unwrap())
        })
        .await;
        let row_lease = Arc::clone(&lease);
        drop(lease);
        assert_eq!(budget.snapshot().retained_bytes, 8);

        context.release_to_collector();
        context.release_to_collector();
        assert_eq!(budget.snapshot().retained_bytes, 0);
        frontier.failed(0);
        let mut lease = Arc::try_unwrap(row_lease).unwrap();
        // Collection detached both the allowance and speculative stop policy.
        lease.resize(u64::MAX).unwrap();
        drop(lease);
        assert_eq!(budget.snapshot().retained_bytes, 0);
        assert_eq!(budget.snapshot().peak_bytes, 8);
    }

    #[tokio::test]
    async fn worker_scopes_remain_distinct_when_polled_in_one_task() {
        let first_budget = PreparationBudget::new(3);
        let second_budget = PreparationBudget::new(7);
        let (first, second) = tokio::join!(
            scope(
                PreparationContext::new(Some(Arc::clone(&first_budget))),
                async {
                    tokio::task::yield_now().await;
                    reserve(3).unwrap()
                }
            ),
            scope(
                PreparationContext::new(Some(Arc::clone(&second_budget))),
                async {
                    tokio::task::yield_now().await;
                    reserve(7).unwrap()
                }
            ),
        );
        assert_eq!(first_budget.snapshot().retained_bytes, 3);
        assert_eq!(second_budget.snapshot().retained_bytes, 7);
        assert!(!parallel_context_active());
        drop((first, second));
        assert_eq!(first_budget.snapshot().retained_bytes, 0);
        assert_eq!(second_budget.snapshot().retained_bytes, 0);
    }

    #[tokio::test]
    async fn serial_and_default_leases_have_no_additional_limit() {
        let mut untracked = PreparationLease::default();
        let budget = PreparationBudget::new(0);
        scope(PreparationContext::new(Some(Arc::clone(&budget))), async {
            untracked.resize(u64::MAX).unwrap();
            assert_eq!(budget.snapshot().retained_bytes, 0);
        })
        .await;
        scope(PreparationContext::new(None), async {
            assert!(!parallel_context_active());
            require_serial().unwrap();
            checkpoint().unwrap();
            let first = reserve(u64::MAX).unwrap();
            let second = reserve(u64::MAX).unwrap();
            drop((first, second));
        })
        .await;
        require_serial().unwrap();
        checkpoint().unwrap();
    }

    #[tokio::test]
    async fn pressure_is_sticky_and_unqualified_paths_request_serial() {
        let budget = PreparationBudget::new(10);
        scope(PreparationContext::new(Some(Arc::clone(&budget))), async {
            assert!(parallel_context_active());
            assert!(is_pressure(&require_serial().unwrap_err()));
            assert!(is_pressure(&checkpoint().unwrap_err()));
            assert!(is_pressure(&reserve(1).unwrap_err()));
        })
        .await;
        assert_eq!(budget.snapshot().retained_bytes, 0);
        assert_eq!(budget.snapshot().reservation_failures, 1);
    }

    #[tokio::test]
    async fn overflowing_reservation_leaves_existing_charge_unchanged() {
        let budget = PreparationBudget::new(u64::MAX);
        scope(PreparationContext::new(Some(Arc::clone(&budget))), async {
            let lease = reserve(u64::MAX).unwrap();
            assert!(is_pressure(&reserve(1).unwrap_err()));
            assert_eq!(budget.snapshot().retained_bytes, u64::MAX);
            drop(lease);
            assert_eq!(budget.snapshot().retained_bytes, 0);
        })
        .await;
    }

    #[test]
    fn only_private_pressure_and_its_context_are_replayed() {
        for suffix in ["", " for node:Person (source snapshot)", ": scanner"] {
            assert!(is_pressure(&OmniError::resource_limit(
                format!("{PRESSURE_RESOURCE}{suffix}"),
                1,
                2,
            )));
        }
        for resource in [
            "branch-merge fenced entity bytes",
            "branch-merge retained validation delta bytes",
            "branch-merge private parallel preparation pressure-foreign",
            "other: branch-merge private parallel preparation pressure",
        ] {
            assert!(!is_pressure(&OmniError::resource_limit(resource, 1, 2)));
        }
        assert!(!is_pressure(&OmniError::manifest_internal(
            PRESSURE_RESOURCE
        )));
    }

    #[test]
    fn scratch_cleanup_waits_for_settlement_and_transfer() {
        let mut scratch = ScratchDirectory::new(tempfile::tempdir().unwrap());
        let path = scratch.path().to_path_buf();
        scratch.begin_write();
        scratch.end_write();
        let dir = scratch;
        assert!(path.exists());
        drop(dir);
        assert!(!path.exists());

        let mut scratch = ScratchDirectory::new(tempfile::tempdir().unwrap());
        let path = scratch.path().to_path_buf();
        scratch.begin_write();
        // A returned write error is still settled: the caller must mark it
        // complete before propagating that error and dropping its owner.
        scratch.end_write();
        drop(scratch);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn scratch_gauges_sum_ready_workers_and_transfer_once_in_all_widths() {
        for parallel in [false, true] {
            let probes = crate::instrumentation::MergeWriteProbes::default();
            let (first, second, first_dir, second_dir) =
                crate::instrumentation::with_merge_write_probes(probes.clone(), async {
                    let budget = PreparationBudget::new(0);
                    let first = PreparationContext::for_window(Arc::clone(&budget), parallel);
                    let second = PreparationContext::for_window(Arc::clone(&budget), parallel);
                    let first_dir = scope(Arc::clone(&first), async {
                        let mut dir = ScratchDirectory::new(tempfile::tempdir().unwrap());
                        dir.add_bytes(3);
                        dir.add_bytes(5);
                        dir
                    })
                    .await;
                    let second_dir = scope(Arc::clone(&second), async {
                        let mut dir = ScratchDirectory::new(tempfile::tempdir().unwrap());
                        dir.add_bytes(13);
                        dir
                    })
                    .await;
                    let reading = probes.merge_preparation_snapshot();
                    assert_eq!((reading.scratch_owners, reading.scratch_bytes), (2, 21));
                    assert_eq!(
                        budget.snapshot().retained_bytes,
                        0,
                        "staged payload is not a resident-memory reservation"
                    );
                    (first, second, first_dir, second_dir)
                })
                .await;
            // Transfer and Drop occur after the task-local probe scope ended.
            first.release_to_collector();
            first.release_to_collector();
            let reading = probes.merge_preparation_snapshot();
            assert_eq!((reading.scratch_owners, reading.scratch_bytes), (1, 13));
            drop(first_dir);
            assert_eq!(probes.merge_preparation_snapshot().scratch_bytes, 13);
            drop(second);
            assert_eq!(
                probes.merge_preparation_snapshot().scratch_bytes,
                13,
                "a ready directory must retain its context after the worker finishes"
            );
            drop(second_dir);
            let reading = probes.merge_preparation_snapshot();
            assert_eq!((reading.scratch_owners, reading.scratch_bytes), (0, 0));
            assert_eq!(
                (reading.peak_scratch_owners, reading.peak_scratch_bytes),
                (2, 21)
            );
        }
    }

    #[tokio::test]
    async fn scratch_and_lease_keep_context_metadata_until_the_last_owner_drops() {
        let probes = crate::instrumentation::MergeWriteProbes::default();
        let (budget, directory, lease) =
            crate::instrumentation::with_merge_write_probes(probes.clone(), async {
                let budget = PreparationBudget::new(20);
                let context = PreparationContext::for_window(Arc::clone(&budget), true);
                let (directory, lease) = scope(context, async {
                    retain(7).unwrap();
                    let lease = reserve(5).unwrap();
                    let mut directory = ScratchDirectory::new(tempfile::tempdir().unwrap());
                    directory.add_bytes(11);
                    (directory, lease)
                })
                .await;
                (budget, directory, lease)
            })
            .await;
        drop(lease);
        assert_eq!(budget.snapshot().retained_bytes, 7);
        assert_eq!(probes.merge_preparation_snapshot().accounted_bytes, 7);
        assert_eq!(probes.merge_preparation_snapshot().scratch_bytes, 11);
        drop(directory);
        assert_eq!(budget.snapshot().retained_bytes, 0);
        let reading = probes.merge_preparation_snapshot();
        assert_eq!(
            (
                reading.accounted_bytes,
                reading.scratch_owners,
                reading.scratch_bytes
            ),
            (0, 0, 0)
        );
    }

    #[derive(Debug, Default)]
    struct IssuedScratchPut {
        accepted: std::sync::atomic::AtomicBool,
        reached: tokio::sync::Notify,
        release: Arc<tokio::sync::Notify>,
        object: std::sync::Mutex<Option<(std::path::PathBuf, Vec<u8>)>>,
        task: std::sync::Mutex<Option<tokio::task::JoinHandle<std::result::Result<(), String>>>>,
    }

    impl Drop for IssuedScratchPut {
        fn drop(&mut self) {
            if let Some(task) = self.task.get_mut().unwrap().take() {
                task.abort();
            }
        }
    }

    #[derive(Debug)]
    struct IssuedScratchStore {
        inner: Arc<dyn object_store::ObjectStore>,
        put: Arc<IssuedScratchPut>,
    }

    impl std::fmt::Display for IssuedScratchStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "IssuedScratchStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for IssuedScratchStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            if location.as_ref().contains("/data/")
                && location.as_ref().ends_with(".lance")
                && !self.put.accepted.swap(true, Ordering::SeqCst)
            {
                let bytes = payload
                    .iter()
                    .flat_map(|part| part.iter().copied())
                    .collect();
                let path = std::path::Path::new("/").join(location.as_ref());
                *self.put.object.lock().unwrap() = Some((path, bytes));
                let inner = Arc::clone(&self.inner);
                let location = location.clone();
                let release = Arc::clone(&self.put.release);
                let (response, receive) = tokio::sync::oneshot::channel();
                // This test adapter owns a request already issued by Lance.
                // Dropping its caller cannot revoke the accepted backend PUT;
                // the test releases and joins that exact real write below.
                let task = tokio::spawn(async move {
                    release.notified().await;
                    let result = inner.put_opts(&location, payload, options).await;
                    let settled = result.as_ref().map(|_| ()).map_err(ToString::to_string);
                    let _ = response.send(result);
                    settled
                });
                *self.put.task.lock().unwrap() = Some(task);
                self.put.reached.notify_one();
                receive.await.expect("test-owned backend PUT must settle")
            } else {
                self.inner.put_opts(location, payload, options).await
            }
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn unfinished_worker_drop_preserves_issued_lance_scratch_write_and_graph_authority() {
        // The full engine facade has the same debug stack requirement as its
        // existing merge_cost owner; concurrency remains one runtime thread.
        std::thread::Builder::new()
            .stack_size(64 * 1024 * 1024)
            .spawn(|| {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(Box::pin(issued_lance_scratch_drop()));
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn issued_lance_scratch_drop() {
        use crate::db::Omnigraph;
        use crate::instrumentation::{
            MergePreparationOptions, MergeWriteProbes, with_merge_preparation_options,
            with_merge_write_probes,
        };
        use crate::loader::LoadMode;
        use std::collections::BTreeMap;
        use std::time::Duration;

        fn graph_files(root: &std::path::Path) -> BTreeMap<std::path::PathBuf, Vec<u8>> {
            let mut files = BTreeMap::new();
            let mut pending = vec![root.to_path_buf()];
            while let Some(directory) = pending.pop() {
                for entry in std::fs::read_dir(directory).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        pending.push(entry.path());
                    } else {
                        files.insert(
                            entry.path().strip_prefix(root).unwrap().to_path_buf(),
                            std::fs::read(entry.path()).unwrap(),
                        );
                    }
                }
            }
            files
        }

        let root = tempfile::tempdir().unwrap();
        let db = Omnigraph::init(
            root.path().to_str().unwrap(),
            "node A { name: String @key value: I32 }\nnode B { name: String @key value: I32 }",
        )
        .await
        .unwrap();
        let rows = |start: usize, end: usize, value: i32| {
            ["A", "B"]
                .into_iter()
                .flat_map(|name| {
                    (start..end).map(move |row| {
                        serde_json::json!({
                            "type": name,
                            "data": {"name": format!("row-{row}"), "value": value}
                        })
                        .to_string()
                    })
                })
                .collect::<Vec<_>>()
                .join("\n")
        };
        db.load("main", &rows(0, 4, 0), LoadMode::Overwrite)
            .await
            .unwrap();
        db.branch_create("source").await.unwrap();
        db.load("source", &rows(0, 2, 10), LoadMode::Merge)
            .await
            .unwrap();
        db.load("main", &rows(2, 4, 20), LoadMode::Merge)
            .await
            .unwrap();
        let before = graph_files(root.path());
        let main_head = db.resolve_snapshot("main").await.unwrap();
        let source_head = db.resolve_snapshot("source").await.unwrap();
        let put = Arc::new(IssuedScratchPut::default());
        let store = Arc::new(IssuedScratchStore {
            inner: Arc::new(object_store::local::LocalFileSystem::new()),
            put: Arc::clone(&put),
        });
        let probes = MergeWriteProbes::default();
        let mut merge = Box::pin(crate::table_store::with_scratch_write_test_store(
            store,
            with_merge_write_probes(
                probes.clone(),
                with_merge_preparation_options(
                    MergePreparationOptions {
                        width: 4,
                        additional_bytes: 128 * 1024 * 1024,
                    },
                    db.branch_merge("source", "main"),
                ),
            ),
        ));
        tokio::time::timeout(Duration::from_secs(30), async {
            tokio::select! {
                _ = put.reached.notified() => {}
                outcome = &mut merge => panic!("merge ended before its issued scratch PUT: {outcome:?}"),
            }
        })
        .await
        .expect("tiny merge must issue a real scratch data write");
        let (object_path, expected_bytes) = put.object.lock().unwrap().clone().unwrap();
        let scratch = object_path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap();
        assert!(
            scratch
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("omnigraph-merge-")
        );
        assert!(!scratch.starts_with(root.path()));
        assert!(probes.merge_preparation_snapshot().scratch_owners > 0);
        assert!(
            !object_path.exists(),
            "accepted PUT is still pending at the adapter"
        );
        drop(merge);
        // This assertion fails if the real StagedTableWriter call site omits
        // begin_write: TempDir would remove this parent before backend release.
        assert!(
            scratch.exists(),
            "in-flight Lance write lost its private directory"
        );
        let dropped = probes.merge_preparation_snapshot();
        assert_eq!(
            (dropped.active, dropped.ready, dropped.uncollected),
            (0, 0, 0)
        );
        assert_eq!(
            (
                dropped.accounted_bytes,
                dropped.scratch_owners,
                dropped.scratch_bytes
            ),
            (0, 0, 0)
        );
        assert_eq!(
            graph_files(root.path()),
            before,
            "drop changed graph-owned files"
        );
        put.release.notify_one();
        let mut task = put.task.lock().unwrap().take().unwrap();
        let settled = tokio::time::timeout(Duration::from_secs(10), &mut task).await;
        let Ok(settled) = settled else {
            task.abort();
            let _ = task.await;
            panic!("issued local PUT must settle");
        };
        settled.unwrap().expect("real local scratch write failed");
        assert_eq!(std::fs::read(&object_path).unwrap(), expected_bytes);
        assert!(
            !expected_bytes.is_empty(),
            "Lance must have encoded a real data object"
        );
        assert_eq!(
            graph_files(root.path()),
            before,
            "late scratch PUT changed graph authority"
        );
        assert_eq!(db.resolve_snapshot("main").await.unwrap(), main_head);
        assert_eq!(db.resolve_snapshot("source").await.unwrap(), source_head);
        // The issued PUT is joined. Only now may the test remove abandoned
        // private scratch; production deliberately leaves it for external cleanup.
        std::fs::remove_dir_all(scratch).unwrap();
    }
}
