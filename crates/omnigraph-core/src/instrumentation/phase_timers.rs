//! Research phase timers for the concurrent-write investigation: process-wide
//! sums of write-path phase durations, read by the throughput instrument
//! (`crates/omnigraph/tests/write_throughput.rs`). Two relaxed atomic adds per
//! recorded phase; nothing reads them in production.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WritePhase {
    /// One mutation attempt, capture to publication or refusal.
    Attempt,
    /// `open_write_txn`.
    Capture,
    /// Query execution into the staging accumulator.
    Execute,
    /// `stage_all` beside the early revalidation probe.
    Stage,
    /// Waiting for the schema/branch/table gates in `commit_all`.
    GateWait,
    /// `revalidate_write_txn_carrying` under the gates.
    Revalidate,
    /// The detached table commits under the gates.
    Detached,
    /// The shared publication helper, coordinator selection to installed view.
    Publish,
    /// A coordinator opened from durable state for a publication.
    CoordinatorOpen,
    /// The publisher's `load_publish_state`.
    PublisherLoad,
    /// The publisher's `commit::overwrite` (data file, transaction, CAS).
    PublisherOverwrite,
    /// From the gates acquired in `commit_all` to the end of the publication.
    GateHold,
}

impl WritePhase {
    pub const ALL: [WritePhase; 12] = [
        WritePhase::Attempt,
        WritePhase::Capture,
        WritePhase::Execute,
        WritePhase::Stage,
        WritePhase::GateWait,
        WritePhase::Revalidate,
        WritePhase::Detached,
        WritePhase::Publish,
        WritePhase::CoordinatorOpen,
        WritePhase::PublisherLoad,
        WritePhase::PublisherOverwrite,
        WritePhase::GateHold,
    ];

    pub fn name(self) -> &'static str {
        match self {
            WritePhase::Attempt => "attempt",
            WritePhase::Capture => "capture",
            WritePhase::Execute => "execute",
            WritePhase::Stage => "stage",
            WritePhase::GateWait => "gate_wait",
            WritePhase::Revalidate => "revalidate",
            WritePhase::Detached => "detached",
            WritePhase::Publish => "publish",
            WritePhase::CoordinatorOpen => "coordinator_open",
            WritePhase::PublisherLoad => "publisher_load",
            WritePhase::PublisherOverwrite => "publisher_overwrite",
            WritePhase::GateHold => "gate_hold",
        }
    }

    fn index(self) -> usize {
        Self::ALL.iter().position(|p| *p == self).unwrap_or(0)
    }
}

static TOTAL_US: [AtomicU64; 12] = [const { AtomicU64::new(0) }; 12];
static COUNT: [AtomicU64; 12] = [const { AtomicU64::new(0) }; 12];

pub fn record(phase: WritePhase, elapsed: Duration) {
    let i = phase.index();
    TOTAL_US[i].fetch_add(elapsed.as_micros() as u64, Ordering::Relaxed);
    COUNT[i].fetch_add(1, Ordering::Relaxed);
}

/// A running phase; `stop` records it. Dropping without `stop` records
/// nothing, so an early `?` return is not counted as a completed phase.
pub struct PhaseTimer {
    phase: WritePhase,
    started: Instant,
}

impl PhaseTimer {
    pub fn start(phase: WritePhase) -> Self {
        Self {
            phase,
            started: Instant::now(),
        }
    }

    pub fn stop(self) {
        record(self.phase, self.started.elapsed());
    }
}

/// `(name, count, total microseconds)` for every phase.
pub fn snapshot() -> Vec<(&'static str, u64, u64)> {
    WritePhase::ALL
        .iter()
        .map(|p| {
            let i = p.index();
            (
                p.name(),
                COUNT[i].load(Ordering::Relaxed),
                TOTAL_US[i].load(Ordering::Relaxed),
            )
        })
        .collect()
}
