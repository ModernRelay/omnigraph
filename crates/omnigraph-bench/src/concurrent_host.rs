//! The measured free-running block. Every worker of a `--- concurrent
//! measured` block loops its statement on a task of its own for the bench
//! section's warmup and duration, each over a clone of the case session. The
//! loop is closed: a worker issues its next statement when the last one is
//! acknowledged, so a slow statement delays the ones behind it and a service
//! time is less than what an arriving caller would have waited (coordinated
//! omission). Every observation carries `driver` and `claim_grade` for that
//! reason, and nothing here is a throughput or latency claim.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use omnigraph::Session;
use omnigraph::instrumentation::{capture_query_io_probes, with_query_io_probes};
use omnigraph_gqt_core::concurrent::SessionKind;
use omnigraph_gqt_core::{ConcurrentStep, PlainHost, SessionOp, run_session};
use serde::{Deserialize, Serialize};
use tokio::task::{JoinError, JoinSet};

use crate::gqt_case::{BlockRun, MEASURED_BLOCK_WORKERS};

/// How the workers of a measured block issue statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Driver {
    ClosedLoop,
}

/// The statement a label's workers loop, written as the step kind a case
/// file names it by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum LabelKind {
    Query,
    Mutate,
    Load,
    #[serde(rename = "branch merge")]
    BranchMerge,
}

impl From<&SessionKind> for LabelKind {
    fn from(kind: &SessionKind) -> Self {
        match kind {
            SessionKind::Query => Self::Query,
            SessionKind::Mutation => Self::Mutate,
            SessionKind::Load(_) => Self::Load,
            SessionKind::Merge(_) => Self::BranchMerge,
        }
    }
}

/// One repetition of a measured block: the run it was given, the window it
/// measured and one row per label, in the order the block declares them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConcurrentObservation {
    pub driver: Driver,
    /// Whether the numbers may be stated as throughput or latency; never
    /// under the closed loop.
    pub claim_grade: bool,
    pub warmup_seconds: u32,
    pub measured_seconds: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub sweep: BTreeMap<String, u64>,
    /// From the end of the warmup to the stop: what the rates divide by.
    pub window_us: u64,
    /// From the stop until the last worker returned its statement in flight.
    pub drain_us: u64,
    pub tokio_unstable: bool,
    pub readback: ReadbackObservation,
    pub labels: Vec<LabelObservation>,
}

/// What the readback after the block read on a fresh handle: the dealt rows
/// acknowledged in warmup and in the window, and how many it compared in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadbackObservation {
    pub rows: u64,
    pub compared: u64,
}

/// One label of the block over the measured window. A statement belongs to
/// the window when it started inside it; `acked_in_drain` of those were
/// acknowledged at or after the declared duration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelObservation {
    pub label: String,
    pub kind: LabelKind,
    pub workers: u32,
    pub acked: u64,
    pub warmup_acked: u64,
    pub acked_in_drain: u64,
    /// `acked` per second of the window, in thousandths.
    pub acked_per_second_millis: u64,
    pub per_worker: PerWorker,
    pub service_time_us: ServiceTime,
    /// The times a statement of the window was prepared again after the
    /// authority moved, inside the engine or by the host.
    pub authority_conflicts: u64,
    /// The acknowledgements of each declared second of the window.
    pub acked_by_second: Vec<u64>,
}

/// The fewest and the most window statements one worker of the label was
/// acknowledged for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PerWorker {
    pub min: u64,
    pub max: u64,
}

/// Nearest-rank service times of the label's window statements, from a
/// statement's start on its worker to its acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceTime {
    pub p50: u64,
    pub p99: u64,
    pub max: u64,
}

/// What the driver hands the host: the window, the labels, and per session
/// of the block how many runs were acknowledged, the warmup's included,
/// which is what the readback asks for.
pub(crate) struct Driven {
    pub window_us: u64,
    pub drain_us: u64,
    pub labels: Vec<LabelObservation>,
    pub acked_runs: Vec<u64>,
}

const WARMUP: u8 = 0;
const MEASURED: u8 = 1;
const STOP: u8 = 2;

/// The service times one label keeps exactly, an even share per worker. A
/// worker past its share keeps every second sample, then every fourth, each
/// standing for its stride.
const SAMPLES_PER_LABEL: usize = 1 << 20;
const _: () = assert!(sample_share(MEASURED_BLOCK_WORKERS) >= 2);

const fn sample_share(workers: usize) -> usize {
    SAMPLES_PER_LABEL / workers
}

struct Clock {
    phase: AtomicU8,
    measured_at: OnceLock<Instant>,
}

struct Samples {
    kept: Vec<u64>,
    share: usize,
    stride: u64,
    seen: u64,
    longest: u64,
}

impl Samples {
    fn up_to(share: usize) -> Self {
        Self {
            kept: Vec::new(),
            share,
            stride: 1,
            seen: 0,
            longest: 0,
        }
    }

    fn record(&mut self, micros: u64) {
        self.longest = self.longest.max(micros);
        if self.seen.is_multiple_of(self.stride) {
            self.kept.push(micros);
            if self.kept.len() == self.share {
                let mut position = 0usize;
                self.kept.retain(|_| {
                    position += 1;
                    position % 2 == 1
                });
                self.stride *= 2;
            }
        }
        self.seen += 1;
    }
}

struct Tally {
    warmup_acked: u64,
    acked: u64,
    in_drain: u64,
    conflicts: u64,
    by_second: Vec<u64>,
    samples: Samples,
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// One worker: its statement again and again until the stop, each run
/// counted in the phase it started in. A failed run stops every worker.
async fn work(
    session: Session,
    op: SessionOp,
    clock: Arc<Clock>,
    seconds: u32,
    share: usize,
) -> Result<Tally, String> {
    let mut tally = Tally {
        warmup_acked: 0,
        acked: 0,
        in_drain: 0,
        conflicts: 0,
        by_second: vec![0; seconds as usize],
        samples: Samples::up_to(share),
    };
    let mut seq = 0u64;
    loop {
        let phase = clock.phase.load(Ordering::Acquire);
        if phase == STOP {
            return Ok(tally);
        }
        let begun = Instant::now();
        let outcome = run_session(&PlainHost, &session, &op, seq).await;
        let ended = Instant::now();
        let report = match outcome {
            Ok(report) => report,
            Err(message) => {
                clock.phase.store(STOP, Ordering::Release);
                return Err(format!(
                    "session `{}` on its run {seq}: {message}",
                    op.label
                ));
            }
        };
        seq += 1;
        if phase == WARMUP {
            tally.warmup_acked += 1;
            continue;
        }
        tally.acked += 1;
        tally.conflicts += u64::from(report.conflicts);
        tally.samples.record(micros(ended.duration_since(begun)));
        let second = clock
            .measured_at
            .get()
            .and_then(|at| usize::try_from(ended.duration_since(*at).as_secs()).ok())
            .and_then(|second| tally.by_second.get_mut(second));
        match second {
            Some(count) => *count += 1,
            None => tally.in_drain += 1,
        }
    }
}

type Ended = Result<(usize, Result<Tally, String>), JoinError>;

/// Sleeps `duration`, or returns the first worker that ended before it,
/// which before the stop is a worker that failed.
async fn hold(
    workers: &mut JoinSet<(usize, Result<Tally, String>)>,
    duration: Duration,
) -> Option<Ended> {
    tokio::select! {
        () = tokio::time::sleep(duration) => None,
        ended = workers.join_next() => ended,
    }
}

/// The rank-`percent` value of `weighted` samples sorted by value, each
/// standing for `weight` statements.
fn nearest_rank(weighted: &[(u64, u64)], percent: u64) -> u64 {
    let total: u128 = weighted.iter().map(|(_, weight)| u128::from(*weight)).sum();
    let rank = (u128::from(percent) * total).div_ceil(100).max(1);
    let mut below = 0u128;
    for (value, weight) in weighted {
        below += u128::from(*weight);
        if below >= rank {
            return *value;
        }
    }
    weighted.last().map_or(0, |(value, _)| *value)
}

/// `acked` statements over `window_us`, per second, in thousandths.
pub(crate) fn rate_millis(acked: u64, window_us: u64) -> u64 {
    if window_us == 0 {
        return 0;
    }
    u64::try_from(u128::from(acked) * 1_000_000_000 / u128::from(window_us)).unwrap_or(u64::MAX)
}

/// Runs the block for `run`: every session of `step` on a task over a clone
/// of `session`, `flip` called once between warmup and the window so the
/// host can discard what its counters saw of the warmup.
pub(crate) async fn drive(
    session: &Session,
    step: &ConcurrentStep,
    run: BlockRun,
    flip: impl FnOnce() -> Result<(), String>,
) -> Result<Driven, String> {
    let clock = Arc::new(Clock {
        phase: AtomicU8::new(WARMUP),
        measured_at: OnceLock::new(),
    });
    let probes = capture_query_io_probes();
    let mut label_workers: BTreeMap<&str, usize> = BTreeMap::new();
    for op in &step.sessions {
        *label_workers.entry(op.group.as_str()).or_default() += 1;
    }
    let mut workers = JoinSet::new();
    for (index, op) in step.sessions.iter().enumerate() {
        let work = work(
            session.clone(),
            op.clone(),
            Arc::clone(&clock),
            run.duration_seconds,
            sample_share(label_workers[op.group.as_str()]),
        );
        let probes = probes.clone();
        workers.spawn(async move {
            let tally = match probes {
                Some(probes) => with_query_io_probes(probes, work).await,
                None => work.await,
            };
            (index, tally)
        });
    }
    let mut ended: Vec<Ended> = Vec::new();
    let mut refusal = None;
    match hold(
        &mut workers,
        Duration::from_secs(u64::from(run.warmup_seconds)),
    )
    .await
    {
        Some(early) => ended.push(early),
        None => match flip() {
            Err(message) => refusal = Some(message),
            Ok(()) => {
                let _ = clock.measured_at.set(Instant::now());
                clock.phase.store(MEASURED, Ordering::Release);
                let window = Duration::from_secs(u64::from(run.duration_seconds));
                ended.extend(hold(&mut workers, window).await);
            }
        },
    }
    let stopped = Instant::now();
    clock.phase.store(STOP, Ordering::Release);
    while let Some(next) = workers.join_next().await {
        ended.push(next);
    }
    let drained = Instant::now();
    let mut tallies: Vec<Option<Tally>> = step.sessions.iter().map(|_| None).collect();
    for worker in ended {
        match worker {
            Ok((index, Ok(tally))) => tallies[index] = Some(tally),
            Ok((_, Err(message))) => refusal = refusal.or(Some(message)),
            Err(error) => {
                refusal = refusal.or(Some(format!("a worker task ended abnormally: {error}")))
            }
        }
    }
    if let Some(message) = refusal {
        return Err(message);
    }
    let measured_at = clock
        .measured_at
        .get()
        .ok_or("the measured window never opened")?;
    let window_us = micros(stopped.duration_since(*measured_at));
    let mut labels: Vec<(LabelObservation, Vec<(u64, u64)>)> = Vec::new();
    let mut acked_runs = Vec::with_capacity(step.sessions.len());
    for (op, tally) in step.sessions.iter().zip(tallies) {
        let tally = tally.ok_or_else(|| format!("session `{}` returned no tally", op.label))?;
        acked_runs.push(tally.warmup_acked + tally.acked);
        if labels.last().is_none_or(|(last, _)| last.label != op.group) {
            labels.push((
                LabelObservation {
                    label: op.group.clone(),
                    kind: LabelKind::from(&op.kind),
                    workers: 0,
                    acked: 0,
                    warmup_acked: 0,
                    acked_in_drain: 0,
                    acked_per_second_millis: 0,
                    per_worker: PerWorker {
                        min: u64::MAX,
                        max: 0,
                    },
                    service_time_us: ServiceTime {
                        p50: 0,
                        p99: 0,
                        max: 0,
                    },
                    authority_conflicts: 0,
                    acked_by_second: vec![0; run.duration_seconds as usize],
                },
                Vec::new(),
            ));
        }
        let (label, weighted) = labels.last_mut().ok_or("the block declares no label")?;
        label.workers += 1;
        label.acked += tally.acked;
        label.warmup_acked += tally.warmup_acked;
        label.acked_in_drain += tally.in_drain;
        label.authority_conflicts += tally.conflicts;
        label.per_worker.min = label.per_worker.min.min(tally.acked);
        label.per_worker.max = label.per_worker.max.max(tally.acked);
        label.service_time_us.max = label.service_time_us.max.max(tally.samples.longest);
        for (second, count) in label.acked_by_second.iter_mut().zip(&tally.by_second) {
            *second += count;
        }
        let stride = tally.samples.stride;
        weighted.extend(
            tally
                .samples
                .kept
                .into_iter()
                .map(|micros| (micros, stride)),
        );
    }
    let labels = labels
        .into_iter()
        .map(|(mut label, mut weighted)| {
            weighted.sort_unstable();
            label.acked_per_second_millis = rate_millis(label.acked, window_us);
            if !weighted.is_empty() {
                label.service_time_us.p50 = nearest_rank(&weighted, 50);
                label.service_time_us.p99 = nearest_rank(&weighted, 99);
            }
            label
        })
        .collect();
    Ok(Driven {
        window_us,
        drain_us: micros(drained.duration_since(stopped)),
        labels,
        acked_runs,
    })
}

/// Whether `observed` is a sample the driver could have produced for `step`
/// under `run` at the binding `sweep`, inside a repetition of `elapsed_us`.
pub(crate) fn validate(
    observed: &ConcurrentObservation,
    step: &ConcurrentStep,
    run: BlockRun,
    sweep: &BTreeMap<String, u64>,
    elapsed_us: u64,
) -> Result<(), String> {
    if observed.driver != Driver::ClosedLoop
        || observed.claim_grade
        || observed.warmup_seconds != run.warmup_seconds
        || observed.measured_seconds != run.duration_seconds
        || observed.sweep != *sweep
    {
        return Err("the block observation names another run than the plan's".into());
    }
    let declared_us = u64::from(run.duration_seconds) * 1_000_000;
    let warmup_us = u64::from(run.warmup_seconds) * 1_000_000;
    if observed.window_us < declared_us
        || observed
            .window_us
            .checked_add(observed.drain_us)
            .and_then(|measured| measured.checked_add(warmup_us))
            .is_none_or(|whole| whole > elapsed_us)
        || observed.readback.compared > observed.readback.rows
    {
        return Err("the block's window, drain and warmup do not fit its repetition".into());
    }
    let mut declared: Vec<(&str, LabelKind, u32)> = Vec::new();
    for op in &step.sessions {
        match declared.last_mut() {
            Some((label, _, workers)) if *label == op.group => *workers += 1,
            _ => declared.push((&op.group, LabelKind::from(&op.kind), 1)),
        }
    }
    if observed.labels.len() != declared.len() {
        return Err("the block observation and the block declare different labels".into());
    }
    let mut dealt = 0u64;
    for (label, (name, kind, workers)) in observed.labels.iter().zip(declared) {
        let counted = label
            .acked_by_second
            .iter()
            .try_fold(label.acked_in_drain, |total, count| {
                total.checked_add(*count)
            });
        let workers_wide = u64::from(workers);
        let times = label.service_time_us;
        if label.label != name
            || label.kind != kind
            || label.workers != workers
            || label.acked_by_second.len() != run.duration_seconds as usize
            || counted != Some(label.acked)
            || label.per_worker.min > label.per_worker.max
            || label
                .per_worker
                .min
                .checked_mul(workers_wide)
                .is_none_or(|least| least > label.acked)
            || label
                .per_worker
                .max
                .checked_mul(workers_wide)
                .is_none_or(|most| most < label.acked)
            || times.p50 > times.p99
            || times.p99 > times.max
            || label.acked_per_second_millis != rate_millis(label.acked, observed.window_us)
        {
            return Err(format!(
                "the row of label `{}` disagrees with the block or with itself",
                label.label
            ));
        }
        match label.kind {
            LabelKind::Load => {
                dealt = dealt
                    .checked_add(label.acked)
                    .and_then(|rows| rows.checked_add(label.warmup_acked))
                    .ok_or("the dealt row count overflows")?;
            }
            LabelKind::Query | LabelKind::Mutate | LabelKind::BranchMerge => {}
        }
    }
    if observed.readback.rows > dealt {
        return Err("the readback read more dealt rows than were acknowledged".into());
    }
    Ok(())
}

/// The largest observation `step` can produce under `run`, for the record's
/// byte budget.
pub(crate) fn upper_bound(step: &ConcurrentStep, run: BlockRun) -> ConcurrentObservation {
    let mut labels: Vec<LabelObservation> = Vec::new();
    for op in &step.sessions {
        if labels.last().is_some_and(|last| last.label == op.group) {
            continue;
        }
        labels.push(LabelObservation {
            label: op.group.clone(),
            kind: LabelKind::from(&op.kind),
            workers: u32::MAX,
            acked: u64::MAX,
            warmup_acked: u64::MAX,
            acked_in_drain: u64::MAX,
            acked_per_second_millis: u64::MAX,
            per_worker: PerWorker {
                min: u64::MAX,
                max: u64::MAX,
            },
            service_time_us: ServiceTime {
                p50: u64::MAX,
                p99: u64::MAX,
                max: u64::MAX,
            },
            authority_conflicts: u64::MAX,
            acked_by_second: vec![u64::MAX; run.duration_seconds as usize],
        });
    }
    ConcurrentObservation {
        driver: Driver::ClosedLoop,
        claim_grade: false,
        warmup_seconds: u32::MAX,
        measured_seconds: u32::MAX,
        sweep: BTreeMap::from([("W".repeat(16), u64::MAX)]),
        window_us: u64::MAX,
        drain_us: u64::MAX,
        tokio_unstable: false,
        readback: ReadbackObservation {
            rows: u64::MAX,
            compared: u64::MAX,
        },
        labels,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_weighs_a_thinned_worker_by_its_stride() {
        let exact: Vec<(u64, u64)> = (1..=100).map(|value| (value, 1)).collect();
        assert_eq!(nearest_rank(&exact, 50), 50);
        assert_eq!(nearest_rank(&exact, 99), 99);
        assert_eq!(nearest_rank(&[(7, 1)], 99), 7);
        let mut mixed = vec![(10, 1), (20, 1), (1000, 8)];
        mixed.sort_unstable();
        assert_eq!(
            nearest_rank(&mixed, 50),
            1000,
            "eight of ten statements took 1000"
        );
        assert_eq!(nearest_rank(&mixed, 10), 10);
    }

    #[test]
    fn a_full_worker_keeps_every_second_sample_and_the_longest() {
        let share = sample_share(MEASURED_BLOCK_WORKERS);
        let mut samples = Samples::up_to(share);
        let total = share as u64 + 4;
        for micros in 0..total {
            samples.record(micros);
        }
        assert_eq!(samples.stride, 2);
        assert_eq!(samples.longest, total - 1);
        assert_eq!(samples.kept.len(), share / 2 + 2);
        assert!(samples.kept.iter().all(|micros| micros % 2 == 0));
    }

    #[test]
    fn a_label_keeps_at_most_its_budget_however_many_workers_share_it() {
        for workers in [1, 3, MEASURED_BLOCK_WORKERS] {
            let share = sample_share(workers);
            assert!(share * workers <= SAMPLES_PER_LABEL, "{workers} workers");
            let mut samples = Samples::up_to(share);
            for micros in 0..3 * share as u64 {
                samples.record(micros);
            }
            assert!(samples.kept.len() < share, "{workers} workers");
        }
    }

    #[test]
    fn a_label_kind_is_written_as_the_step_kind_of_a_case_file() {
        let written = |kind: LabelKind| serde_json::to_value(kind).unwrap();
        assert_eq!(written(LabelKind::Query), "query");
        assert_eq!(written(LabelKind::Mutate), "mutate");
        assert_eq!(written(LabelKind::Load), "load");
        assert_eq!(written(LabelKind::BranchMerge), "branch merge");
    }

    #[test]
    fn a_rate_is_statements_per_second_in_thousandths() {
        assert_eq!(rate_millis(3, 2_000_000), 1500);
        assert_eq!(rate_millis(1, 3_000_000), 333);
        assert_eq!(rate_millis(5, 0), 0);
    }
}
