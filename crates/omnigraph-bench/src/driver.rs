//! The driver: it issues the statements of a measured block and times each
//! one. The host calls `drive` when the selected step is a `--- concurrent
//! measured` block; a single operation is issued once by the GQT executor and
//! no driver runs. Every worker of the block loops its statement on a task of
//! its own for the bench section's warmup and duration, each over a session
//! of its own: a clone of the case session in process, an HTTP client against
//! a server. The loop is closed: a worker issues its next statement when the
//! last one is acknowledged, so a slow statement delays the ones behind it
//! and a service time is less than what an arriving caller would have waited
//! (coordinated omission). Every observation carries `driver` and
//! `claim_grade` for that reason, and nothing here is a throughput or latency
//! claim.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use omnigraph::Session;
use omnigraph::instrumentation::{capture_query_io_probes, with_query_io_probes};
use omnigraph_gqt_core::concurrent::SessionKind;
use omnigraph_gqt_core::{
    ConcurrentStep, PlainHost, ServedSession, SessionOp, SessionReport, run_session,
};
use serde::{Deserialize, Serialize};
use tokio::task::{JoinError, JoinSet};

use crate::gqt_case::BlockRun;

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
    /// authority moved, inside the engine or by the host. Against a server
    /// only the host's own re-runs are counted: what the server prepared
    /// again behind an acknowledged write is not on the wire.
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

/// Service times of the label's window statements, from a statement's start
/// on its worker to its acknowledgement. `p50` and `p99` are nearest-rank
/// over every statement: exact under 256 µs, and above that less than 1/128
/// over the time of the statement at that rank. `max` is exact.
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

/// A service time under `FINE` µs has a bucket of its own, and every power
/// of two from there up is cut into `FINE` equal buckets: exact under
/// 256 µs, and above that narrower than 1/128 of the times it counts.
const FINE_BITS: u32 = 7;
const FINE: usize = 1 << FINE_BITS;
const BUCKETS: usize = (u64::BITS - FINE_BITS + 1) as usize * FINE;

fn bucket(micros: u64) -> usize {
    let coarse = micros
        .checked_ilog2()
        .and_then(|log| log.checked_sub(FINE_BITS));
    match coarse {
        None => micros as usize,
        Some(shift) => (shift as usize + 1) * FINE + ((micros >> shift) as usize - FINE),
    }
}

/// The longest service time bucket `index` counts.
fn bucket_top(index: usize) -> u64 {
    let (group, step) = (index / FINE, (index % FINE) as u64);
    match group.checked_sub(1) {
        None => step,
        Some(shift) => ((FINE as u64 + step) << shift) | ((1u64 << shift) - 1),
    }
}

struct Clock {
    phase: AtomicU8,
    measured_at: OnceLock<Instant>,
}

/// The service times of window statements as a count per bucket: every
/// statement is counted, and the longest is kept as measured.
struct ServiceTimes {
    counts: Vec<u64>,
    longest: u64,
}

impl ServiceTimes {
    fn new() -> Self {
        Self {
            counts: vec![0; BUCKETS],
            longest: 0,
        }
    }

    fn record(&mut self, micros: u64) {
        self.longest = self.longest.max(micros);
        self.counts[bucket(micros)] += 1;
    }

    fn absorb(&mut self, other: &Self) {
        self.longest = self.longest.max(other.longest);
        for (count, more) in self.counts.iter_mut().zip(&other.counts) {
            *count += more;
        }
    }

    /// The nearest-rank `percent` time: the top of the bucket the statement
    /// of that rank is counted in, or the longest time when that is less.
    fn nearest_rank(&self, percent: u64) -> u64 {
        let total: u128 = self.counts.iter().map(|count| u128::from(*count)).sum();
        let rank = (u128::from(percent) * total).div_ceil(100).max(1);
        let mut below = 0u128;
        for (index, count) in self.counts.iter().enumerate() {
            below += u128::from(*count);
            if below >= rank {
                return bucket_top(index).min(self.longest);
            }
        }
        0
    }
}

struct Tally {
    warmup_acked: u64,
    acked: u64,
    in_drain: u64,
    conflicts: u64,
    by_second: Vec<u64>,
    times: ServiceTimes,
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

/// What a worker loops: one run of its session's statement, under the one
/// rule of [`run_session`] wherever the statement runs.
pub(crate) trait SessionRunner: Send + 'static {
    fn run_once(
        &self,
        op: &SessionOp,
        seq: u64,
    ) -> impl Future<Output = Result<SessionReport, String>> + Send;
}

impl SessionRunner for Session {
    async fn run_once(&self, op: &SessionOp, seq: u64) -> Result<SessionReport, String> {
        run_session(&PlainHost, self, op, seq).await
    }
}

impl SessionRunner for ServedSession {
    async fn run_once(&self, op: &SessionOp, seq: u64) -> Result<SessionReport, String> {
        self.run(op, seq).await
    }
}

/// One worker: its statement again and again until the stop, each run
/// counted in the phase it started in. A failed run stops every worker.
async fn work(
    runner: impl SessionRunner,
    op: SessionOp,
    clock: Arc<Clock>,
    seconds: u32,
) -> Result<Tally, String> {
    let mut tally = Tally {
        warmup_acked: 0,
        acked: 0,
        in_drain: 0,
        conflicts: 0,
        by_second: vec![0; seconds as usize],
        times: ServiceTimes::new(),
    };
    let mut seq = 0u64;
    loop {
        let phase = clock.phase.load(Ordering::Acquire);
        if phase == STOP {
            return Ok(tally);
        }
        let begun = Instant::now();
        let outcome = runner.run_once(&op, seq).await;
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
        tally.times.record(micros(ended.duration_since(begun)));
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

/// `acked` statements over `window_us`, per second, in thousandths.
pub(crate) fn rate_millis(acked: u64, window_us: u64) -> u64 {
    if window_us == 0 {
        return 0;
    }
    u64::try_from(u128::from(acked) * 1_000_000_000 / u128::from(window_us)).unwrap_or(u64::MAX)
}

/// Opens the measured window: `flip`, then the instant the rates count from.
fn open_window(clock: &Clock, flip: &impl Fn() -> Result<(), String>) -> Result<(), String> {
    flip()?;
    let _ = clock.measured_at.set(Instant::now());
    clock.phase.store(MEASURED, Ordering::Release);
    Ok(())
}

/// Runs the block for `run`: every session of `step` on a task over its
/// runner, `runners` in the order the block declares its sessions. `flip` is
/// called where the window opens, so the host can discard what its counters
/// saw of a warmup; with none, no worker has run.
pub(crate) async fn drive(
    runners: Vec<impl SessionRunner>,
    step: &ConcurrentStep,
    run: BlockRun,
    flip: impl Fn() -> Result<(), String>,
) -> Result<Driven, String> {
    if runners.len() != step.sessions.len() {
        return Err(format!(
            "the block declares {} sessions and was given {} runners",
            step.sessions.len(),
            runners.len()
        ));
    }
    let clock = Arc::new(Clock {
        phase: AtomicU8::new(WARMUP),
        measured_at: OnceLock::new(),
    });
    let probes = capture_query_io_probes();
    let warmup = Duration::from_secs(u64::from(run.warmup_seconds));
    if warmup.is_zero() {
        open_window(&clock, &flip)?;
    }
    let mut workers = JoinSet::new();
    for (index, (runner, op)) in runners.into_iter().zip(&step.sessions).enumerate() {
        let work = work(runner, op.clone(), Arc::clone(&clock), run.duration_seconds);
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
    if !warmup.is_zero() {
        match hold(&mut workers, warmup).await {
            Some(early) => ended.push(early),
            None => refusal = open_window(&clock, &flip).err(),
        }
    }
    if ended.is_empty() && refusal.is_none() {
        let window = Duration::from_secs(u64::from(run.duration_seconds));
        ended.extend(hold(&mut workers, window).await);
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
    let mut labels: Vec<(LabelObservation, ServiceTimes)> = Vec::new();
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
                ServiceTimes::new(),
            ));
        }
        let (label, times) = labels.last_mut().ok_or("the block declares no label")?;
        label.workers += 1;
        label.acked += tally.acked;
        label.warmup_acked += tally.warmup_acked;
        label.acked_in_drain += tally.in_drain;
        label.authority_conflicts += tally.conflicts;
        label.per_worker.min = label.per_worker.min.min(tally.acked);
        label.per_worker.max = label.per_worker.max.max(tally.acked);
        for (second, count) in label.acked_by_second.iter_mut().zip(&tally.by_second) {
            *second += count;
        }
        times.absorb(&tally.times);
    }
    let labels = labels
        .into_iter()
        .map(|(mut label, times)| {
            label.acked_per_second_millis = rate_millis(label.acked, window_us);
            label.service_time_us = ServiceTime {
                p50: times.nearest_rank(50),
                p99: times.nearest_rank(99),
                max: times.longest,
            };
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
            || (run.warmup_seconds == 0 && label.warmup_acked != 0)
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
    use std::sync::atomic::AtomicU64;

    #[test]
    fn nearest_rank_counts_every_statement_of_every_worker() {
        let mut label = ServiceTimes::new();
        assert_eq!(label.nearest_rank(99), 0, "no statement, no time");
        let (mut odd, mut even) = (ServiceTimes::new(), ServiceTimes::new());
        for micros in 1..=100u64 {
            let worker = if micros % 2 == 1 { &mut odd } else { &mut even };
            worker.record(micros);
        }
        label.absorb(&odd);
        label.absorb(&even);
        assert_eq!(label.nearest_rank(50), 50);
        assert_eq!(label.nearest_rank(99), 99);
        assert_eq!(label.longest, 100);
        let mut coarse = ServiceTimes::new();
        coarse.record(1000);
        coarse.record(5000);
        assert_eq!(
            coarse.nearest_rank(50),
            1003,
            "the top of the bucket that counts 1000"
        );
        assert_eq!(coarse.nearest_rank(99), 5000, "never over the longest");
    }

    #[test]
    fn a_bucket_is_exact_under_256_micros_and_narrower_than_a_128th_above() {
        let times = [
            0,
            1,
            127,
            128,
            255,
            256,
            257,
            1000,
            65_535,
            65_536,
            3_600_000_000,
            u64::MAX - 1,
            u64::MAX,
        ];
        let mut last = 0;
        for micros in times {
            let index = bucket(micros);
            assert!(last <= index && index < BUCKETS, "{micros}");
            last = index;
            let top = bucket_top(index);
            assert!(
                micros <= top && top - micros <= micros >> FINE_BITS,
                "{micros} is counted in a bucket up to {top}"
            );
            assert!(micros >= 256 || top == micros, "{micros}");
        }
        assert_eq!(bucket(u64::MAX), BUCKETS - 1);
    }

    #[test]
    fn a_slow_statement_on_every_second_run_is_in_the_p99() {
        let mut times = ServiceTimes::new();
        for run in 0..8192u64 {
            times.record(if run % 2 == 0 { 10 } else { 1000 });
        }
        assert_eq!((times.nearest_rank(50), times.nearest_rank(99)), (10, 1000));
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

    const EVERY_KIND: &str = r#"# issue: none
# notes: One label of every statement kind a block admits.

--- runner
timeout_ms: 60000
environments:
  - target: omnigraph-engine
    storage: local-filesystem

--- bench
warmup_seconds: 0
duration_seconds: 1

--- concurrent measured
writer x2: load generate: v1 seed: 42 mode: append
  tables:
    - kind: node
      name: Chunk
      rows: 40960
      commits: 10
      columns:
        slug: {kind: key, prefix: cw-, width: 5}
        n: {kind: ordinal, start: 1000, step: 1}
reader: query seeded_row() { match { $c: Chunk { slug: "seed-07" } } return { $c.n } }
toucher: query touch() { update Chunk set { n: 7 } where slug = "seed-07" }
folder: branch merge side into main
--- expect
writer: ok
reader: ok
toucher: ok
folder: ok
"#;

    /// A runner that answers from a script: every run reports `conflicts`,
    /// the run `fails_at` fails, and `runs` counts the runs it began.
    struct Scripted {
        conflicts: u32,
        fails_at: Option<u64>,
        runs: Arc<AtomicU64>,
    }

    impl SessionRunner for Scripted {
        async fn run_once(&self, op: &SessionOp, seq: u64) -> Result<SessionReport, String> {
            self.runs.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(5)).await;
            if self.fails_at == Some(seq) {
                return Err(format!("the scripted {} was refused", op.kind.name()));
            }
            Ok(SessionReport {
                conflicts: self.conflicts,
            })
        }
    }

    /// One runner per session of `step`, scripted by `script` as the conflicts
    /// of a run and the run that fails, with the count of runs each one began.
    fn scripted(
        step: &ConcurrentStep,
        script: impl Fn(&SessionOp) -> (u32, Option<u64>),
    ) -> (Vec<Scripted>, Vec<Arc<AtomicU64>>) {
        step.sessions
            .iter()
            .map(|op| {
                let (conflicts, fails_at) = script(op);
                let runs = Arc::new(AtomicU64::new(0));
                let runner = Scripted {
                    conflicts,
                    fails_at,
                    runs: Arc::clone(&runs),
                };
                (runner, runs)
            })
            .unzip()
    }

    fn began(runs: &[Arc<AtomicU64>]) -> Vec<u64> {
        runs.iter()
            .map(|runs| runs.load(Ordering::Relaxed))
            .collect()
    }

    #[tokio::test]
    async fn every_statement_kind_is_driven_and_reported_under_its_label() {
        let case = omnigraph_gqt_core::parse_case("every_kind", EVERY_KIND).unwrap();
        let step = case.measured_block().unwrap();
        let (runners, runs) = scripted(step, |op| {
            let conflicts = match op.kind {
                SessionKind::Mutation => 1,
                SessionKind::Merge(_) => 2,
                SessionKind::Query | SessionKind::Load(_) => 0,
            };
            (conflicts, None)
        });
        let run = BlockRun {
            warmup_seconds: 0,
            duration_seconds: 1,
        };
        let driven = drive(runners, step, run, || Ok(())).await.unwrap();
        assert!(
            driven.window_us >= 1_000_000,
            "the window is the declared second"
        );
        let began = began(&runs);
        assert_eq!(
            driven.acked_runs, began,
            "every run a worker began is acknowledged, the ones in the drain too"
        );
        let rows: Vec<_> = driven
            .labels
            .iter()
            .map(|label| (label.label.as_str(), label.kind, label.workers))
            .collect();
        assert_eq!(
            rows,
            [
                ("writer", LabelKind::Load, 2),
                ("reader", LabelKind::Query, 1),
                ("toucher", LabelKind::Mutate, 1),
                ("folder", LabelKind::BranchMerge, 1),
            ],
            "one row per label, in declaration order"
        );
        assert_eq!(driven.labels[0].acked, began[0] + began[1]);
        for (label, per_run) in driven.labels.iter().zip([0, 0, 1, 2]) {
            let name = &label.label;
            assert!(label.acked > 0, "{name}: no statement was acknowledged");
            assert_eq!(
                label.authority_conflicts,
                label.acked * per_run,
                "{name}: the conflicts every run reports are summed under its label"
            );
            assert_eq!(
                label.acked_by_second.iter().sum::<u64>() + label.acked_in_drain,
                label.acked,
                "{name}: every acknowledgement lands in the second or in the drain"
            );
        }
    }

    #[tokio::test]
    async fn a_failed_run_ends_the_block_and_names_its_session_and_its_run() {
        let case = omnigraph_gqt_core::parse_case("every_kind", EVERY_KIND).unwrap();
        let step = case.measured_block().unwrap();
        let (runners, runs) = scripted(step, |op| {
            (0, matches!(op.kind, SessionKind::Merge(_)).then_some(3))
        });
        let run = BlockRun {
            warmup_seconds: 0,
            duration_seconds: 30,
        };
        let started = Instant::now();
        let refusal = drive(runners, step, run, || Ok(())).await.err();
        assert_eq!(
            refusal.as_deref(),
            Some("session `folder` on its run 3: the scripted branch merge was refused")
        );
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the block ends at the failed run, not after its declared 30 s"
        );
        assert_eq!(began(&runs)[4], 4, "the failed run is the session's last");
    }

    #[tokio::test]
    async fn a_block_is_driven_with_one_runner_per_session() {
        let case = omnigraph_gqt_core::parse_case("every_kind", EVERY_KIND).unwrap();
        let step = case.measured_block().unwrap();
        let (mut runners, runs) = scripted(step, |_| (0, None));
        runners.pop();
        let run = BlockRun {
            warmup_seconds: 0,
            duration_seconds: 1,
        };
        let refusal = drive(runners, step, run, || Ok(())).await.err();
        assert_eq!(
            refusal.as_deref(),
            Some("the block declares 5 sessions and was given 4 runners")
        );
        assert_eq!(began(&runs), [0; 5], "no worker ran");
    }
}
