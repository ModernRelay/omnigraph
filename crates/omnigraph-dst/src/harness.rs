//! The universe runner.
//!
//! One universe = one seeded, single-threaded, in-memory world driving a real
//! `Omnigraph` through its production write path, checked by an edge-aware
//! differential model + referential-integrity oracle, CONTINUOUS verification
//! (exact-model equality every third op), versioned values, seeded storage
//! faults at the StorageAdapter seam (latency sleeps VIRTUAL time), a
//! read-only third audit view, and seed logging for CI reproducibility.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;
use std::sync::{Arc, Mutex};

use omnigraph::changes::{ChangeFilter, ChangeOp, EntityKind};
use omnigraph::db::{InitOptions, Omnigraph, ReadTarget, SnapshotId};
use omnigraph::error::{OmniError, Result as OmniResult};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};

use crate::detectors::{self, Channel, Detector, ObservationSource, Oracle};
use crate::fixtures::{
    MUTATION_QUERIES, TEST_DATA, TEST_SCHEMA, knows_pairs, knows_pairs_bound_target,
    knows_pairs_on, knows_pairs_target, knows_pairs_target_mode, mixed_params, mutate_on,
    person_jsonl, person_rows, person_rows_on, person_rows_target, physical_view_on, query_main,
    schema_with_extras,
};
use crate::rand::SplitMix64;

/// Process-static debug toggles; the DST_PREDICT_LOG / DST_OP_LOG env
/// vars are also honored when set before process start.
pub mod debug_knobs {
    use std::sync::atomic::AtomicBool;
    pub static PREDICT_LOG: AtomicBool = AtomicBool::new(false);
    pub static OP_LOG: AtomicBool = AtomicBool::new(false);
}

/// Stack size for every universe-running thread (in-suite and the lane B
/// child): engine futures overflow the 2 MiB default test stack, and a
/// dedicated big thread is the systemic fix — spawn sites use this
/// constant instead of re-minting the number.
pub const UNIVERSE_STACK_BYTES: usize = 16 * 1024 * 1024;

/// Reset every process-global fault/schedule slot a PREVIOUS universe
/// could have leaked. Runs at the TOP of every universe runner: the
/// end-of-universe clears sit after the judged block and are skipped
/// when a violation panics, and libtest continues past a failed
/// `#[serial]` test — so without this, a panicked faulty universe arms
/// the next universe's realm (weather/kill into a concurrent race, an
/// arbiter into a sequential universe, a bytes canary into anything).
/// ALSO runs at `run_universe_caught`'s exit (both outcomes): the
/// start-side clear only protects the next UNIVERSE, and a manual
/// (non-universe) test following a panicked faulty universe would
/// otherwise inherit its armed weather.
pub(crate) fn clear_process_slots() {
    crate::lance_faults::set_active(None);
    crate::lance_faults::set_kill(None);
    crate::lance_faults::set_seam_scheduler(None);
    crate::lance_faults::set_bytes_canary(None);
    FOREIGN_SIDECAR_ROWS.lock().unwrap().clear();
}

/// The plain (unpaused, unseeded) current-thread tokio runtime used by
/// lane B parents and the dst_child binary — real time, enable_time only.
/// The in-suite seeded/paused builders stay per-site: their extra knobs
/// (`start_paused`, `rng_seed`, `build_local`) are scenario-specific.
pub fn plain_current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread tokio runtime")
}

// --------------------------------------------- detector tags --
// One const per firing site family: recorded violations carry these by
// construction (a red without a detector does not exist — guard 2 in `detectors.rs`). The
// census golden file (`detector_census.txt`) is generated from the same
// enums; sources here must match the oracle's census row.

const fn on(c: Channel, o: Oracle) -> Detector {
    Detector {
        source: ObservationSource::Store(c),
        oracle: o,
    }
}
pub const DET_WORLD: Detector = on(Channel::Query, Oracle::WorldDifferential);
pub const DET_MEMBERSHIP: Detector = on(Channel::Query, Oracle::MembershipQuery);
pub const DET_RO_AUDIT: Detector = on(Channel::Query, Oracle::ReadOnlyAudit);
pub const DET_PHYSICAL: Detector = on(Channel::Physical, Oracle::PhysicalExport);
pub const DET_HISTORY: Detector = on(Channel::History, Oracle::HistoryDifferential);
pub const DET_TRAVERSAL: Detector = on(Channel::Query, Oracle::TraversalModeDifferential);
pub const DET_SESSION: Detector = on(Channel::Session, Oracle::SessionDifferential);
pub const DET_ARBITRATION_QUERY: Detector = on(Channel::Query, Oracle::OpArbitration);
pub const DET_ARBITRATION_PHYSICAL: Detector = on(Channel::Physical, Oracle::OpArbitration);
pub const DET_MERGE_PREDICTION: Detector = on(Channel::Claim, Oracle::MergePrediction);
pub const DET_LEGAL_CLAIM: Detector = on(Channel::Claim, Oracle::LegalRejection);
pub const DET_ACK_LOSS: Detector = on(Channel::Claim, Oracle::AckLossArbitration);
pub const DET_CRASH_CONTRACT: Detector = on(Channel::Query, Oracle::CrashContract);
pub const DET_BIRTH: Detector = on(Channel::Claim, Oracle::BirthContract);
pub const DET_RECOVERY_OBLIGATION: Detector = on(Channel::Physical, Oracle::RecoveryObligation);
pub const DET_RESIDUE: Detector = on(Channel::Physical, Oracle::ResidueObligation);
pub const DET_LIVE_WRITE_AVAILABILITY: Detector =
    on(Channel::Session, Oracle::LiveWriteAvailability);
pub const DET_MAINTENANCE: Detector = on(Channel::Query, Oracle::MaintenanceObligations);
pub const DET_OCC: Detector = on(Channel::History, Oracle::CommitIdUniqueness);
pub const DET_LIVENESS: Detector = Detector {
    source: ObservationSource::Time,
    oracle: Oracle::LivenessBound,
};
pub const DET_REPLAY: Detector = Detector {
    source: ObservationSource::HarnessOutput,
    oracle: Oracle::StrictReplay,
};

const NAMES: [&str; 8] = ["w0", "w1", "w2", "w3", "w4", "w5", "w6", "w7"];
/// The load alphabet — names the merge-load ops upsert. Distinct from
/// the mutation alphabets so load/mutation interference stays diagnosable in
/// op logs.
const LOAD_NAMES: [&str; 4] = ["l0", "l1", "l2", "l3"];
/// HOSTILE-ALPHABET lever — valid but nasty keys.
const HOSTILE: [&str; 8] = [
    "emoji-\u{1F600}",
    "quote-inside",
    "space here",
    "Person",
    "match",
    "\u{00e9}\u{00e8}\u{00ea}",
    "veryveryveryveryveryveryveryveryveryveryveryverylongkeyname000",
    "tabhere",
];
pub(crate) const FAULT_MARKER: &str = "injected fault (dst)";
pub(crate) const KILL_MARKER: &str = "killed at write (dst)";
pub(crate) const ACK_LOSS_MARKER: &str = "lost acknowledgement (dst)";
/// Latent sector errors: honest like `FAULT_MARKER`, but LOCATION-indexed
/// and PERSISTENT (semantics on `FaultPlan::latent_read_pct`).
pub(crate) const LATENT_MARKER: &str = "latent sector error (dst)";

// ---------------------------------------------------------------- scenario --

/// Seeded storage-fault plan: each write-class storage call rolls the plan's
/// own SplitMix64 stream — `error_pct` of them fail with a marked error,
/// `latency_pct` sleep 1..=max_latency_ms of VIRTUAL time first.
#[derive(Clone, Debug)]
pub struct FaultPlan {
    pub seed: u64,
    pub error_pct: u64,
    /// Slatedb-derived doctrine (probe/list failpoints): faults on READ-class
    /// calls too — reads and listings (`read_fault` call sites) — one seam knob instead of
    /// per-point instrumentation.
    pub read_error_pct: u64,
    pub latency_pct: u64,
    pub max_latency_ms: u64,
    /// also storm the LANCE realm (table IO through the
    /// interposed provider). Opt-in: lance-realm universes sit outside the
    /// replay envelope (the REPLAY-ENVELOPE NOTE on `UniverseReport`).
    /// Oracles hold either way; adapter-realm-only plans stay fully
    /// replayable.
    pub lance_realm: bool,
    /// ack-loss: this % of SUCCESSFUL write-class calls have
    /// their acknowledgement lost — the effect is DURABLE (delegation
    /// happened), but the caller receives a marked error. The inverse of
    /// `error_pct`'s clean loss. Pressure-tests retry idempotency (see
    /// `client_retry` and the CAS note on `write_text_if_match`). Adapter realm only
    /// in v1 (the Lance realm's retry jitter sits outside the replay
    /// envelope anyway). Rolls draw from the plan's rng stream ONLY when
    /// this knob is nonzero, so zero-knob plans keep their exact
    /// pre-existing draw sequences (pinned tests unchanged).
    pub ack_loss_pct: u64,
    /// CLIENT RETRY: when a workload op fails with a lost
    /// acknowledgement, the harness plays the real client's move and
    /// retries the SAME op once. The retry runs against the client's own
    /// (usually durable) success — upserts must converge, a re-merge is
    /// an empty-delta merge. The retry's error surface is held to
    /// `is_legal_rejection` STRICTLY: any novel
    /// retry-after-own-success error shape is a first-contact verdict, not
    /// a tolerated blur. Reconcile still arbitrates the settled world.
    pub client_retry: bool,
    /// CORRUPTION AXIS (read tier) — READ-TIME BIT ROT: this % of successful
    /// content-read calls (adapter realm) return MUTATED text — the store
    /// lies, no error anywhere. Read-path-only by design: stored bytes stay
    /// true, so oracle suspension keeps every judged read clean and
    /// the model needs no damage ledger yet (persisted verbs carry the damage ledger below).
    /// The engine's response is judged detected-or-harmless: an op failure
    /// whose reads crossed the damage ledger is an ATTRIBUTED DETECTION
    /// (legal, recorded in `UniverseReport.corruption_detections` for
    /// typed-vs-raw triage); silent model divergence is a violation. Same
    /// draw discipline as `ack_loss_pct`.
    pub corrupt_read_pct: u64,
    /// CORRUPTION AXIS (read tier) — TRUNCATED READS: this % of successful
    /// content-read calls return a strict prefix (a partial read presented
    /// as complete). Same draw discipline and judgment as bit rot.
    pub truncate_read_pct: u64,
    /// CORRUPTION AXIS (read tier) — LATENT SECTOR ERRORS: this % of content-read
    /// calls permanently POISON their URI — that read and every later read
    /// of the same URI fail with `LATENT_MARKER` while faults are active.
    /// Honest-but-located: an error the caller sees, but persistent and
    /// location-indexed, so retrying cannot outrun it. Same draw discipline.
    pub latent_read_pct: u64,
    /// CORRUPTION AXIS (persisted tier) — WRITE-TIME CORRUPTION: this % of content
    /// writes store MUTATED text (digit-aware bit rot on the payload,
    /// pre-delegation). PERSISTED damage: every later reader — including
    /// fault-suspended recovery reads, which the call-path gates cannot
    /// protect — receives the same wrong bytes. A write census measured:
    /// a standard universe's adapter-realm content writes are exactly the
    /// `__recovery/` sidecars, so this verb IS sidecar weather. Same
    /// draws-only-when-nonzero discipline as every read-tier knob.
    pub corrupt_write_pct: u64,
    /// CORRUPTION AXIS (persisted tier) — LOST WRITES: this % of eligible write-class
    /// calls are silently dropped — the decorator returns SUCCESS without
    /// delegating (success reported, effect absent: the exact inverse of
    /// `ack_loss_pct`, the claim channel's claimed-but-invisible /
    /// Lost Write phenomenon). Applies to write_text / write_text_if_absent
    /// / rename_text / delete / delete_prefix; the CAS
    /// (`write_text_if_match`) is EXCLUDED — losing it would require
    /// fabricating a version token, a consistency lie that belongs with
    /// the staleness axis. A lost write never reaches the store, so it is NOT
    /// counted by the kill enumerator (count-landed-writes-only). A lost
    /// DELETE plants sidecar residue — judged by the attributed-residue
    /// reopen-heal assert at the final audit, not excused.
    pub lose_write_pct: u64,
    /// CORRUPTION AXIS (persisted tier) — MISDIRECTED WRITES: this % of content writes
    /// land at the WRONG key — same directory, filename prefixed
    /// `dstm-` (extension preserved) — so the intended object is missing
    /// AND a foreign object appears in the same keyspace (a sidecar
    /// listing will SEE it — filename-robustness probe for recovery).
    /// write_text / write_text_if_absent only in v1.
    pub misdirect_write_pct: u64,
    /// BOUNDED STALENESS on content reads: this % of eligible
    /// reads (read_text / read_text_if_exists(_bounded) / exists /
    /// read_text_versioned) serve the key's state AS OF an earlier store
    /// tick (lag drawn 1..=max_lag_ticks) instead of the head. The store
    /// LIES about recency, never about content: values are true old values
    /// (`read_text_versioned` serves the pair-consistent old (content,
    /// token) — see the method). A stale read may RESURRECT a deleted key
    /// (zombie read) or serve absence for a young key. Keys with no
    /// recorded history at the as-of tick serve fresh (pre-armed world
    /// unknown). The CAS itself and all writes stay STRICT at head (the
    /// real providers' contract). Models: a cache layer, replicated
    /// deployments (S3 CRR, multisite secondaries), weak S3-compatible
    /// dialects. Draws only when nonzero.
    pub stale_read_pct: u64,
    /// BOUNDED STALENESS on listings: this % of
    /// list_dir(_bounded) calls serve membership AS OF an earlier tick —
    /// keys deleted since then still listed (zombie listings), keys created
    /// since then absent. One lag draw per call (the whole listing is an
    /// internally consistent old world; different calls may disagree — the
    /// load-balanced-frontend shape). Same draw discipline.
    pub stale_list_pct: u64,
    /// the staleness bound: max ticks of lag a stale serving
    /// may carry (a tick = one landed write-class call). Bounded staleness
    /// is a DECLARED modeling choice: the oracle always knows what the
    /// store converges to; "correct up to lag k" is the checked claim.
    pub max_lag_ticks: u64,
}

/// Zero-fault plan — identical to `FaultPlan::none()`, exposed so tests can
/// spell partial plans as `FaultPlan { seed, corrupt_read_pct: 8, ..Default::default() }`
/// without chasing every new knob.
impl Default for FaultPlan {
    fn default() -> Self {
        Self::none()
    }
}

impl FaultPlan {
    /// All-zero plan: used when the crash-state enumeration needs the
    /// storage wrapper installed but no fault weather was requested.
    pub(crate) fn none() -> Self {
        Self {
            seed: 0,
            error_pct: 0,
            read_error_pct: 0,
            latency_pct: 0,
            max_latency_ms: 1,
            lance_realm: false,
            ack_loss_pct: 0,
            client_retry: false,
            corrupt_read_pct: 0,
            truncate_read_pct: 0,
            latent_read_pct: 0,
            corrupt_write_pct: 0,
            lose_write_pct: 0,
            misdirect_write_pct: 0,
            stale_read_pct: 0,
            stale_list_pct: 0,
            max_lag_ticks: 4,
        }
    }
}

/// The ALICE-style crash-state enumerator's switch (mechanism:
/// kill-at-kth-write). One counter counts durable write-class storage calls
/// across BOTH realms (adapter + Lance); at write #k the switch turns DEAD —
/// the k-th write is LOST and every later storage call in either realm fails
/// — until the harness revives it for recovery (the process-restart analog).
/// Counting shares the fault gates: armed after init/fixtures, suspended
/// during oracle checks and reconcile, so recovery's own writes are not kill
/// candidates in v1 (the `recovery_crash` double fault covers that axis).
/// `k = usize::MAX` never fires: the count-only probe that learns a
/// workload's total write count W.
#[derive(Debug)]
pub struct KillState {
    k: usize,
    /// LANE B whitebox: COMPLETION-CUT mode. The counter counts
    /// SUCCESSFUL DURABLE COMPLETIONS — that count is the completion-cut
    /// coordinate N used everywhere in lane B (attempts, precondition
    /// misses, and backend errors never count). At completion #c the
    /// state fsyncs a barrier line and PARKS the thread: the write is
    /// durable, the engine never hears the return, nothing else is in
    /// flight (single-threaded child, awaited calls). The PARENT then
    /// delivers the real SIGKILL to the frozen child. c = 0 parks before
    /// the first mutating call is forwarded. Only ever true in the
    /// dst_child binary, never in-suite.
    real: bool,
    writes: std::sync::atomic::AtomicUsize,
    completions: std::sync::atomic::AtomicUsize,
    /// Mutating calls currently between admission and return, both
    /// realms. The completion-cut barrier asserts this is exactly 1
    /// (the parked call itself): the zero-in-flight claim is CHECKED at
    /// every cut, not argued from single-threadedness (Lance pools and
    /// spawn_blocking threads exist even under a current_thread runtime).
    in_flight: std::sync::atomic::AtomicUsize,
    /// Where the barrier line is appended (the child's op log). Set by
    /// the rig in real mode; the fsync makes the line SIGKILL-proof.
    barrier_path: Mutex<Option<String>>,
    dead: std::sync::atomic::AtomicBool,
    enabled: std::sync::atomic::AtomicBool,
    suspended: std::sync::atomic::AtomicBool,
    hit: std::sync::atomic::AtomicBool,
    killed_label: Mutex<Option<String>>,
}

impl KillState {
    pub(crate) fn new(k: usize) -> Arc<Self> {
        Self::build(k, false)
    }

    /// LANE B whitebox: a kill state that cuts at durable completion #c
    /// via barrier-and-park (see the `real` field doc).
    pub(crate) fn new_real(c: usize) -> Arc<Self> {
        Self::build(c, true)
    }

    fn build(k: usize, real: bool) -> Arc<Self> {
        Arc::new(Self {
            k,
            real,
            writes: std::sync::atomic::AtomicUsize::new(0),
            completions: std::sync::atomic::AtomicUsize::new(0),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            barrier_path: Mutex::new(None),
            dead: std::sync::atomic::AtomicBool::new(false),
            enabled: std::sync::atomic::AtomicBool::new(false),
            suspended: std::sync::atomic::AtomicBool::new(false),
            hit: std::sync::atomic::AtomicBool::new(false),
            killed_label: Mutex::new(None),
        })
    }

    /// Real mode: where to append the barrier line.
    pub(crate) fn set_barrier_path(&self, path: &str) {
        *self.barrier_path.lock().unwrap() = Some(path.to_string());
    }

    /// Freeze at the cut: write the barrier evidence, then park forever;
    /// the parent's SIGKILL is the only exit (the loop re-parks on
    /// spurious wakeups). Line durability rationale: `oplog` module doc.
    /// The line is appended via a second O_APPEND handle on the op-log
    /// file; safe because this function diverges on a single-threaded
    /// child, so the main handle (non-append, own offset) never writes
    /// afterward. Unlike `oplog::emit`, a failed barrier write here parks
    /// silently and surfaces as the parent's watchdog panic — a panic in
    /// the parked child would exit it and forge a non-barrier death.
    fn barrier_and_park(&self, c: usize, op: &str, uri: &str) -> ! {
        let in_flight = self.in_flight.load(std::sync::atomic::Ordering::SeqCst);
        assert!(
            in_flight == 1,
            "completion-cut barrier at #{c}: {in_flight} mutating calls in flight \
             (must be exactly the parked one; a concurrent writer breaks the cut's \
             zero-in-flight guarantee)"
        );
        if let Some(path) = self.barrier_path.lock().unwrap().clone()
            && let Ok(mut f) = std::fs::OpenOptions::new().append(true).open(&path)
        {
            // ONE write syscall for the whole line (oplog's contract):
            // per-fragment writes would let the polling parent observe a
            // torn prefix, kill, and strand an unparseable barrier line.
            let line = format!("{}\n", crate::oplog::barrier_line(c, op, uri, in_flight));
            let _ = f.write_all(line.as_bytes());
            let _ = f.sync_data();
        }
        loop {
            std::thread::park();
        }
    }

    /// RAII admission gauge for one mutating call (see `in_flight`).
    pub(crate) fn enter_write(self: &Arc<Self>) -> InFlightWrite {
        self.in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        InFlightWrite(Arc::clone(self))
    }

    fn enable(&self) {
        self.enabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn suspend(&self) {
        self.suspended
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    fn resume(&self) {
        self.suspended
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn dead(&self) -> bool {
        self.dead.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether the counter is currently armed (enabled and not suspended)
    /// — the workload window. Read by the write census to tag rows.
    pub(crate) fn counting(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::SeqCst)
            && !self.suspended.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Modeled process-restart analog: storage answers again, and the
    /// enumerator is done for this universe (one death per universe, like
    /// crash windows). Same process, same caches — lane B's real death
    /// has no revive; its recovery runs in a fresh process.
    pub(crate) fn revive_and_disarm(&self) {
        self.dead.store(false, std::sync::atomic::Ordering::SeqCst);
        self.enabled
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    pub(crate) fn writes_observed(&self) -> usize {
        self.writes.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn hit(&self) -> bool {
        self.hit.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn killed_label(&self) -> Option<String> {
        self.killed_label.lock().unwrap().clone()
    }

    /// Count a durable write that is actually REACHING the store — call
    /// AFTER any fault roll (ordering rationale on `write_fault`).
    /// `Ok` = the write may proceed; `Err` = the write is LOST (death fires
    /// here, or the process is already dead).
    pub(crate) fn on_write(&self, op: &str, uri: &str) -> Result<(), String> {
        if self.dead() {
            return Err(format!("{KILL_MARKER}: post-mortem {op} {uri}"));
        }
        if !self.enabled.load(std::sync::atomic::Ordering::SeqCst)
            || self.suspended.load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(());
        }
        let n = self
            .writes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        if self.real {
            // Completion-cut mode: attempts never kill. The one attempt-side
            // duty is c = 0 — block before forwarding the FIRST mutating
            // call: park at the first attempt, before delegation.
            if self.k == 0 && n == 1 {
                self.barrier_and_park(0, op, uri);
            }
            return Ok(());
        }
        if n == self.k {
            self.dead.store(true, std::sync::atomic::Ordering::SeqCst);
            self.hit.store(true, std::sync::atomic::Ordering::SeqCst);
            *self.killed_label.lock().unwrap() = Some(format!("{op} {uri}"));
            return Err(format!("{KILL_MARKER}: write #{n} {op} {uri}"));
        }
        Ok(())
    }

    /// COMPLETION ordinal (real mode only): count a SUCCESSFUL durable
    /// completion — called AFTER the inner store confirmed the write. At
    /// completion #c: barrier-and-park (the write is durable; the return
    /// never reaches the engine). Errors never get here, so N counts
    /// successful durable completions by construction. Lane A's modeled
    /// enumeration is attempt-based (`on_write`); TODO(#527): migrate it
    /// to completion ordinals plus a lane A/B agreement check (a v2
    /// tracking issue renumbers this marker).
    pub(crate) fn on_completion(&self, op: &str, uri: &str) {
        if !self.real
            || !self.enabled.load(std::sync::atomic::Ordering::SeqCst)
            || self.suspended.load(std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let n = self
            .completions
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        if self.k > 0 && n == self.k {
            self.barrier_and_park(n, op, uri);
        }
    }

    /// Successful durable completions counted so far (the completion-cut
    /// coordinate N — see the `real` field doc).
    pub(crate) fn completions_observed(&self) -> usize {
        self.completions.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Post-mortem refusal, any call class — a dead process performs nothing.
    /// Checked FIRST on every call (before fault rolls and counting).
    pub(crate) fn refuse_if_dead(&self, op: &str, uri: &str) -> Result<(), String> {
        if self.dead() {
            return Err(format!("{KILL_MARKER}: post-mortem {op} {uri}"));
        }
        Ok(())
    }
}

/// Held for the duration of one mutating call; drop = the call returned
/// (either way). See `KillState::in_flight`.
pub(crate) struct InFlightWrite(Arc<KillState>);

impl Drop for InFlightWrite {
    fn drop(&mut self) {
        self.0
            .in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// LANE B rig — the dst_child binary's composition of an optional
/// completion-cut kill and optional fault weather over the base adapter,
/// exposing the shared `KillState` and weather state for the Lance realm
/// (`lance_faults::set_kill` / `set_active` + `install_file`). One
/// counter, both realms; arming discipline on [`RealKillRig::arm`].
/// There is deliberately no disarm/revive dual: the rig lives in a child
/// whose only exit is SIGKILL or process exit — the process boundary IS
/// the disarm (unlike the modeled lane's `revive_and_disarm`).
pub struct RealKillRig {
    failing: Arc<FailingStorage>,
    kill: Option<Arc<KillState>>,
    lance_weather: Option<Arc<crate::lance_faults::LanceFaultState>>,
}

impl RealKillRig {
    /// Weather + optional whitebox kill: transient faults compose with
    /// real death — a mid-op storage error followed (or not) by SIGKILL.
    /// Lane B weather stays in the CLEAN classes (errors/latency); the
    /// honesty-axis and corruption knobs remain lane A's, because the
    /// replay judge's `err`-must-not-apply reading is strict and ack
    /// loss would legitimately break it.
    pub fn new(
        base: Arc<dyn StorageAdapter>,
        die_at_write: Option<usize>,
        weather: Option<FaultPlan>,
    ) -> Self {
        let kill = die_at_write.map(KillState::new_real);
        let lance_weather = weather
            .as_ref()
            .filter(|p| p.lance_realm)
            .map(crate::lance_faults::LanceFaultState::from_plan);
        let failing = Arc::new(FailingStorage::new(
            base,
            weather.unwrap_or_else(FaultPlan::none),
            lance_weather.clone(),
            kill.clone(),
        ));
        Self {
            failing,
            kill,
            lance_weather,
        }
    }

    /// The wrapped adapter the child hands to `init_with_storage`.
    pub fn storage(&self) -> Arc<dyn StorageAdapter> {
        self.failing.clone()
    }

    /// The kill state for `lance_faults::set_kill` (Lance realm counting).
    pub fn kill_state(&self) -> Option<Arc<KillState>> {
        self.kill.clone()
    }

    /// The Lance-realm weather state for `lance_faults::set_active`.
    pub fn lance_weather(&self) -> Option<Arc<crate::lance_faults::LanceFaultState>> {
        self.lance_weather.clone()
    }

    /// Arm counting and weather. The one home of the arming discipline:
    /// workload children arm after init+fixtures (the lane A gate); the
    /// recover-mode child arms BEFORE its open, so recovery's own writes
    /// become kill candidates (the real `recovery_crash` cell).
    pub fn arm(&self) {
        self.failing.enable();
    }

    /// Successful durable completions counted so far: the completion-cut
    /// coordinate N (the probe logs it as the `N ` op-log line).
    pub fn completions_observed(&self) -> usize {
        self.kill
            .as_ref()
            .map(|k| k.completions_observed())
            .unwrap_or(0)
    }

    /// Real mode: where the barrier line is appended (the child's op log).
    pub fn set_barrier_path(&self, path: &str) {
        if let Some(k) = &self.kill {
            k.set_barrier_path(path);
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct Scenario {
    pub seed: u64,
    pub ops: usize,
    /// Deterministic crash: before op `.0`, schedule a crash at failpoint
    /// `.1` ("return").
    pub crash_at: Option<(usize, &'static str)>,
    /// TARGETED SCHEDULING: schedule the crash at failpoint
    /// `.0` on the `.1`-th sampled op whose KIND matches the window's family
    /// (`window_matches`), instead of a blind fixed index — merge windows get
    /// hit by actual merges. At most one crash per universe, exactly like
    /// `crash_at`, and just as deterministic: the trigger is a pure function
    /// of the seeded op stream.
    pub crash_on_match: Option<(&'static str, usize)>,
    /// CROSSING PROBE: with `crash_on_match` set, install a
    /// RECORD-ONLY callback instead of an injected failure — the op executes
    /// normally and `UniverseReport.crossed` says whether execution actually
    /// walked through the window. The hunt uses this to tell
    /// "scheduled-but-never-reached" apart from "crossed-but-ABSORBED":
    /// windows whose injected error the engine heals transparently (the
    /// branch post_native ambiguity classifiers, phase-D sidecar delete) are
    /// invisible to the `crashes > 0` signal yet fully exercised.
    pub probe_only: bool,
    /// Storage-fault plan for the whole universe (None = clean storage).
    pub faults: Option<FaultPlan>,
    /// DOUBLE-FAULT lever: schedule this failpoint DURING the post-crash
    /// recovery sweep (the reopen), one time, then let a second reopen
    /// finish clean. Tests "does recovery recover from its own death?".
    pub recovery_crash: Option<&'static str>,
    /// HOSTILE-ALPHABET lever: draw workload names from a hostile alphabet (unicode, long,
    /// keyword-like, whitespace) instead of the clean w0..w7 set.
    pub hostile: bool,
    /// widened workload: adds schema-evolution, mid-life bulk
    /// load (merge/append/fork-from-base), and refresh/sync ops to the
    /// sampler (die 12 → 16). Gated so every pre-existing pinned seed keeps
    /// its exact op stream.
    pub wide: bool,
    /// Kill-at-kth-write: die at durable write #k (1-based). `usize::MAX`
    /// = count-only probe (learns W, never dies). Mechanism: `KillState`.
    pub die_at_write: Option<usize>,
    /// MILESTONE-CONSTRAINED REACH: build the precondition state
    /// this window needs by WEAVING its milestone ops into the seeded stream
    /// (positions and parameters still seeded). One
    /// seeded universe reaches the window by construction instead of by
    /// luck. `None` = ordinary sampling.
    pub reach_target: Option<&'static str>,
    /// PERSISTENT crossing probe: a record-only
    /// callback on this window held for the WHOLE universe (workload ops,
    /// reconcile recoveries, final audit), feeding `UniverseReport.crossed`.
    /// Unlike `probe_only` (which scopes the probe to the single matched op
    /// for the hunt's per-op attribution), this answers the census question
    /// "did ANY execution in this universe walk the line" — and it composes
    /// with a REAL `crash_on_match` setup crash, which is how the
    /// orphan-reclaim windows (classify.fresh_read, fork.before_reclaim)
    /// and the recovery.* internals get their preconditions built.
    pub probe_window: Option<&'static str>,
    /// SENSITIVITY KNOB (test-only red proof): force
    /// the maintenance-obligation RERUN to fail through a real engine
    /// failpoint, proving the idempotent-convergence oracle can go red.
    /// Never set outside the sensitivity test.
    pub fail_maintenance_rerun: bool,
    /// READER-ABLATION knobs (reborn-branch cache-poison triage), default off
    /// (all read machinery runs). Ablation localized the read-corruption class's trigger to the
    /// harness's between-op read traffic: a faithful op-level hand replay
    /// PASSES while the seeded universe FAILS, so one of the readers arms
    /// the trigger. Each knob removes exactly one reader so the ablation
    /// matrix (`dst_reborn_branch_cache_poison_reader_ablation`) can name it. TRIAGE-ONLY:
    /// pins never set these — ablated universes waive the corresponding
    /// oracles.
    /// Skip the per-op history snapshot capture (reads).
    /// NOTE: the session oracle's legality set shrinks to current-state
    /// when history is empty — interpret session reds in this cell with
    /// that in mind.
    pub ablate_history: bool,
    /// Skip the mid-run session checks (fresh read-only
    /// opens plus bystander reads). The bystander handle itself still opens once at
    /// setup.
    pub ablate_sessions: bool,
    /// Skip the mid-run three-arm traversal differential (the
    /// forced-indexed / forced-CSR / bound arms — incl. the documented
    /// graph-index cache warming).
    pub ablate_mode_arms: bool,
    /// Skip the every-3rd-op whole-world verification + membership query
    /// (the continuous-verification traversals).
    pub ablate_verify: bool,
    /// Finer split of `ablate_verify` (the matrix named verify as the
    /// arming reader): skip ONLY the whole-world model compare (the
    /// every-branch traversals) while keeping the main-only membership
    /// query — a green here pins the arming read to the BRANCH traversal.
    pub ablate_world_match: bool,
    /// Positional pin: run the whole-world compare at EXACTLY this op
    /// index and nowhere else (overrides the cadence; `ablate_world_match`
    /// ignored when set). A red with a single position = the arming is one
    /// read of one specific state — the minimal repro's coordinates.
    /// (Measured 2026-08-14: GREEN at 26 and at 2 on seed 10177 — one
    /// read never arms it; the trigger needs ACCUMULATED reads.)
    pub world_match_only_at: Option<usize>,
    /// Dose-response: world-match at the normal cadence but only for ops
    /// at or after this index (None = from the start). Walking this down
    /// from the victim op bounds HOW MANY accumulated reads the trigger
    /// needs. (Measured 2026-08-14: from-14 still GREEN on both faces
    /// while the full cadence is RED — the arming reads are EARLY,
    /// before op 14.)
    pub world_match_from: Option<usize>,
    /// The bracket's other jaw: world-match at the normal cadence but only
    /// for ops < this index. Early-only RED + late-only GREEN = the
    /// arming reads are entirely in the FIRST LIFE's window.
    pub world_match_until: Option<usize>,
    /// KEEP-SERVING phase (issue #554): on a `RecoveryRequired` failure,
    /// DEFER reconcile's reopen and keep the SAME handle serving — the
    /// long-lived-server shape where the handle is never reopened on first
    /// refusal. The value is the budget: this many refusals naming one
    /// operation id fire the live-write-availability detector. The watch
    /// resolves (deferred arbitration runs) on a success, any other
    /// failure — different-id refusals and scheduled crashes included,
    /// EXCEPT a clean-recovery-state maintenance refusal, which continues
    /// the watch without counting — or loop end. `client_retry` is
    /// mutually scoped out (enforced by assert at universe start).
    /// 0 = off; standing constraint: the knob must never perturb an
    /// existing pin's op stream or rng draws.
    pub keep_serving_ops: usize,
}

/// the milestone steps a window's family needs before its op
/// can execute through the target line. Each step forces one op (with
/// seeded parameters) into the stream; the sampler emits pending steps at
/// seeded positions and force-drains any remainder near the loop end, so
/// the precondition is GUARANTEED while the surrounding life stays random.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Milestone {
    /// Ensure the milestone branch exists (create from main).
    EnsureBranch,
    /// Put divergent data on the milestone branch (an insert).
    DataOnBranch,
    /// Merge the milestone branch into main (the branch_merge families).
    MergeBranch,
    /// Delete the milestone branch (the branch_delete families).
    DeleteBranch,
    /// A load-fork op (the fork/load-fork families).
    ForkLoad,
    /// A plain mutation on main (mutation/publish/classify families).
    MutateMain,
    /// Delete a base-fixture person on main (delete-shaped windows).
    DeleteFixtureOnMain,
    /// Delete a base-fixture person on the milestone branch — puts a DELETE
    /// in the branch's merge delta (the rewrite-with-deletes merge route).
    DeleteFixtureOnBranch,
    /// `ensure_indices` on main (the non-fork ensure_indices windows).
    EnsureIndicesMain,
    /// `ensure_indices` on the FRESH milestone branch — every table pin is
    /// first-touch, the deferred-fork ensure_indices route.
    EnsureIndicesBranch,
    /// `cleanup` on main (branch-snapshot resolution / fork reconciliation /
    /// table GC run against whatever state earlier milestones built).
    CleanupMain,
}

const MILESTONE_BRANCH: &str = "mstone";

/// The milestone sequence for a target window: window-specific recipes first
/// (preconditions named from the engine sites), then the family
/// default. Empty = not milestone territory (schema is quarantined; init/open
/// are owned by birth universes). The recovery.* internals and the
/// orphan-reclaim windows (classify.fresh_read, fork.before_reclaim) DO get
/// steps here — theirs build the PRIMARY crash's precondition; the census
/// pairs them with a real setup crash (`census_setup` in the instrument).
fn milestone_steps(window: &str) -> Vec<Milestone> {
    use Milestone::*;
    // Window-specific recipes (2026-08-12).
    match window {
        // Delete-shaped mutation window: guarantee a DeletePerson exists.
        "mutation.delete_node_pre_primary_delete" => {
            return vec![MutateMain, DeleteFixtureOnMain];
        }
        // First-touch fork route: fresh branch, then its first data op.
        "mutation.post_sidecar_pre_fork"
        | "mutation.post_fork_pre_commit"
        | "fork.post_create_pre_open" => {
            return vec![EnsureBranch, DataOnBranch];
        }
        // Orphan-ref territory: the census crashes the branch delete at
        // `before_table_cleanup`, whose injected failure the engine SWALLOWS
        // (branch gone, per-table fork refs leak — omnigraph.rs's own doc
        // names the cleanup reconciler as the backstop). Then:
        // - a cleanup walks the leaked refs (reconcile_fork + the
        //   classify fresh-authority read);
        // - re-creating the branch and writing to it collides with the
        //   leaked ref on the write path (reclaim_orphaned_fork_and_refork).
        "classify.fresh_read" | "cleanup.reconcile_fork" => {
            return vec![EnsureBranch, DataOnBranch, DeleteBranch, CleanupMain];
        }
        "fork.before_reclaim" => {
            return vec![
                EnsureBranch,
                DataOnBranch,
                DeleteBranch,
                EnsureBranch,
                DataOnBranch,
            ];
        }
        // Merge whose delta carries a DELETE — the rewrite-with-deletes
        // route. The MutateMain runs BEFORE the branch is cut so the branch
        // base holds an edge-free person the branch can delete without the
        // merge becoming predicted-conflict.
        "branch_merge.rewrite_after_delete_pre_confirm" => {
            return vec![
                MutateMain,
                EnsureBranch,
                DataOnBranch,
                DeleteFixtureOnBranch,
                MergeBranch,
            ];
        }
        // ensure_indices deferred-fork route: put data on the branch first
        // (forks ONE table and places it) so the branch ensure_indices has
        // work to do and the remaining tables are first-touch.
        "ensure_indices.post_sidecar_pre_fork" | "ensure_indices.post_table_effect" => {
            return vec![EnsureBranch, DataOnBranch, EnsureIndicesBranch];
        }
        // cleanup with state to work on: a live branch fork.
        "cleanup.resolve_branch_snapshot" => {
            return vec![EnsureBranch, DataOnBranch, CleanupMain];
        }
        "cleanup.table_gc" | "cleanup.post_recovery_check_pre_gates" => {
            return vec![MutateMain, DeleteFixtureOnMain, CleanupMain];
        }
        // Recovery internals: build the PRIMARY crash's precondition (the
        // census adds the crash itself).
        "recovery.before_roll_forward_publish" => {
            return vec![EnsureBranch, DataOnBranch, DataOnBranch, MergeBranch];
        }
        _ => {}
    }
    let family = window.split('.').next().unwrap_or(window);
    match family {
        "branch_merge" => vec![EnsureBranch, DataOnBranch, DataOnBranch, MergeBranch],
        "branch_delete" => vec![EnsureBranch, DataOnBranch, DeleteBranch],
        "branch_create" | "branch_control" => vec![EnsureBranch],
        "fork" => vec![ForkLoad],
        "load" if window == "load.post_branch_create_pre_stage" => vec![ForkLoad],
        "load" => vec![DataOnBranch], // LoadMerge/Append on main
        "mutation" | "graph_publish" | "publish" | "classify" | "recovery" => vec![MutateMain],
        "ensure_indices" => vec![MutateMain, EnsureIndicesMain],
        "cleanup" => vec![MutateMain, CleanupMain],
        "optimize" => vec![MutateMain],
        _ => Vec::new(),
    }
}

/// Emit the next pending milestone op if it is due (seeded coin, or forced
/// when the remaining op budget is tight), else `None` to fall through to
/// ordinary sampling. `progress` is advanced as steps are emitted.
fn milestone_op(
    steps: &[Milestone],
    progress: &mut usize,
    ops_left: usize,
    rng: &mut SplitMix64,
    world: &WorldModel,
    next_ver: &mut i64,
) -> Option<WorldOp> {
    if *progress >= steps.len() {
        return None;
    }
    let remaining = steps.len() - *progress;
    // Force when the budget would not otherwise fit the remaining steps
    // (leave one slot for the target op itself); else a seeded coin.
    let due = ops_left <= remaining + 1 || rng.below(2) == 0;
    if !due {
        return None;
    }
    let step = steps[*progress];
    let wop = match step {
        Milestone::EnsureBranch => {
            if world.branches.contains_key(MILESTONE_BRANCH) {
                *progress += 1; // already satisfied — advance, no op this tick
                return None;
            }
            WorldOp::BranchCreate {
                name: MILESTONE_BRANCH.to_string(),
            }
        }
        Milestone::DataOnBranch => {
            let branch = if world.branches.contains_key(MILESTONE_BRANCH) {
                MILESTONE_BRANCH
            } else {
                "main"
            };
            *next_ver += 1;
            let name = format!("ms{}", *next_ver);
            WorldOp::Data {
                branch: branch.to_string(),
                op: Op::InsertV {
                    name,
                    age: (rng.below(80) + 10) as i64,
                    ver: *next_ver,
                },
            }
        }
        Milestone::MergeBranch => {
            if !world.branches.contains_key(MILESTONE_BRANCH) {
                return None; // precondition not built yet; wait
            }
            WorldOp::BranchMerge {
                source: MILESTONE_BRANCH.to_string(),
            }
        }
        Milestone::DeleteBranch => {
            if !world.branches.contains_key(MILESTONE_BRANCH) {
                return None;
            }
            WorldOp::BranchDelete {
                name: MILESTONE_BRANCH.to_string(),
            }
        }
        Milestone::ForkLoad => {
            *next_ver += 1;
            WorldOp::LoadFork {
                branch: MILESTONE_BRANCH.to_string(),
                people: vec![(format!("mf{}", *next_ver), 30, *next_ver)],
            }
        }
        Milestone::MutateMain => {
            *next_ver += 1;
            WorldOp::Data {
                branch: "main".to_string(),
                op: Op::InsertV {
                    name: format!("mm{}", *next_ver),
                    age: 40,
                    ver: *next_ver,
                },
            }
        }
        Milestone::DeleteFixtureOnMain => {
            // Prefer a base-fixture person; fall back to any survivor. If
            // main is empty (hostile filler), skip the step honestly.
            let candidate = ["Bob", "Charlie", "Diana", "Alice"]
                .iter()
                .find(|n| world.main.persons.contains_key(**n))
                .map(|n| n.to_string())
                .or_else(|| world.main.persons.keys().next().cloned());
            match candidate {
                Some(name) => WorldOp::Data {
                    branch: "main".to_string(),
                    op: Op::DeletePerson { name },
                },
                None => {
                    *progress += 1;
                    return None;
                }
            }
        }
        Milestone::DeleteFixtureOnBranch => {
            // A delete of a row COMMON to base and branch puts a delete in
            // the merge delta (the rewrite-with-deletes route). Prefer an
            // EDGE-FREE common row: deleting a person with live edges makes
            // the eventual merge predicted-conflict (referential repair),
            // which legally rejects before the rewrite route runs. Wait for
            // the branch; skip if no common row survives.
            let slot = world.branches.get(MILESTONE_BRANCH)?;
            let common = || {
                slot.base
                    .persons
                    .keys()
                    .filter(|n| slot.state.persons.contains_key(*n))
            };
            let candidate = common()
                .find(|n| {
                    !slot
                        .state
                        .edges
                        .iter()
                        .any(|(from, to)| from == *n || to == *n)
                })
                .or_else(|| common().next())
                .cloned();
            match candidate {
                Some(name) => WorldOp::Data {
                    branch: MILESTONE_BRANCH.to_string(),
                    op: Op::DeletePerson { name },
                },
                None => {
                    *progress += 1;
                    return None;
                }
            }
        }
        Milestone::EnsureIndicesMain => WorldOp::Data {
            branch: "main".to_string(),
            op: Op::EnsureIndices,
        },
        Milestone::EnsureIndicesBranch => {
            if !world.branches.contains_key(MILESTONE_BRANCH) {
                return None; // wait for EnsureBranch
            }
            WorldOp::Data {
                branch: MILESTONE_BRANCH.to_string(),
                op: Op::EnsureIndices,
            }
        }
        Milestone::CleanupMain => WorldOp::Data {
            branch: "main".to_string(),
            op: Op::Cleanup,
        },
    };
    *progress += 1;
    Some(wop)
}

/// (branch, persons, edges) triples, main first then branches in name order —
/// the whole observable store.
pub type WorldState = Vec<(String, Vec<(String, i64, i64)>, Vec<(String, String)>)>;

/// Everything observable about a finished universe; same-seed universes must
/// produce EQUAL reports.
///
/// REPLAY-ENVELOPE NOTE (2026-08-12): universes with `FaultPlan.lance_realm`
/// are NOT replay-compared at all — Lance's jittered retry backoff
/// (`lance-core utils/backoff.rs:65` draws `rand::rng()`) plus the entropy
/// shim's cross-thread draw-order limit can flip retry OUTCOMES between
/// same-seed runs (per-universe `entropy::arm` + `ThreadRng::reseed` and
/// self-synchronizing fault decisions shrank but did not close it). Full
/// report equality is the contract for every other universe class and
/// restores for lance-realm ones when the upstream Lance deterministic-mode
/// PR lands.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniverseReport {
    pub end_state: Vec<(String, i64, i64)>,
    pub edges: Vec<(String, String)>,
    /// The full world (main + surviving branches).
    pub world: WorldState,
    pub commit_ids: Vec<String>,
    /// Raw JSON of a full compiler→DataFusion read, ROW ORDER INCLUDED.
    pub query_digest: String,
    pub crashes: usize,
    /// Crossing probe (`Scenario::probe_only`): execution walked through the
    /// scheduled window at least once.
    pub crossed: bool,
    /// main's physical-vs-logical edge delta at universe end — EMPTY by
    /// construction since the #474 fix (self-loops are ordinary visible
    /// edges); the oracle pins it at zero, so a nonempty value is a
    /// regression to stored-but-traversal-hidden rows.
    pub ghost_edges: Vec<(String, String)>,
    /// main's recorded commit history (every head advance during
    /// the universe) — the commits the history oracle re-read and
    /// verified against the model's memory of each moment (the slice at or
    /// after the retention horizon).
    pub history_commits: Vec<String>,
    /// the bystander session's observed-history-index trail —
    /// one entry per session check (the session-level `commit_mins`);
    /// replay-compared like everything else in the report.
    pub bystander_trail: Vec<usize>,
    /// Continuous-verification passes performed during the run.
    pub verified: usize,
    /// Ops that failed for a LEGAL reason (injected fault, RI rejection,
    /// model-predicted merge conflict, keep-serving deferral refusals —
    /// the watched streak and the maintenance-barrier spelling).
    pub legal_rejections: usize,
    /// errors the Lance-realm injector actually delivered —
    /// evidence the table realm saw weather (0 in clean universes).
    pub lance_realm_injected: usize,
    /// durable writes counted across both realms while the kill
    /// enumerator was armed (0 without a kill/count state). For a count-only
    /// probe this is the workload's total write count W.
    pub writes_observed: usize,
    /// the crash state was actually manufactured (write #k was
    /// reached and the death fired).
    pub crash_state_hit: bool,
    /// acknowledgements the adapter realm actually lost —
    /// evidence the inverse fault direction (effect durable, ack lost)
    /// saw action (0 without `ack_loss_pct`).
    pub acks_lost: usize,
    /// Client retries performed after ack-lost ops
    /// (0 without `client_retry`).
    pub client_retries: usize,
    /// maintenance deaths whose obligation pass ran (rerun
    /// converged + per-op obligation held). Bite evidence — 0 in universes
    /// whose deaths never landed in a maintenance op.
    pub maintenance_reruns: usize,
    /// content reads returned with read-time bit rot (0 without
    /// `corrupt_read_pct`). Bite evidence for the corruption axis; the
    /// crossing discipline — injected damage an engine read never consumed
    /// is a structural miss, not a trial, so these count DELIVERED lies.
    pub reads_corrupted: usize,
    /// content reads returned truncated (0 without
    /// `truncate_read_pct`).
    pub reads_truncated: usize,
    /// latent sector errors delivered — first poisonings AND
    /// every later refused read of a poisoned URI.
    pub latent_errors: usize,
    /// ATTRIBUTED DETECTIONS — op failures whose reads crossed
    /// the damage ledger during the op, so the (unmarkable, engine-born)
    /// error is attributed to injected corruption by overlap. Each row is
    /// `op<i> <op-kind>: <error-snippet>`; first-contact triage classifies
    /// typed vs raw detection per row (the retention-horizon precedent:
    /// raw untyped Lance leaks are issue-candidates, not violations).
    pub corruption_detections: Vec<String>,
    /// Persisted-verb bite counters — content writes
    /// stored mutated, write-class calls silently dropped (success
    /// fabricated), writes landed at the wrong key.
    pub writes_corrupted: usize,
    pub writes_lost: usize,
    pub writes_misdirected: usize,
    /// Reads that touched a persisted-damage URI —
    /// the CONSUMPTION count (a stored lie nothing reads is a
    /// structural miss, not a trial). Counts through suspension, since
    /// stored damage flows through any read.
    pub persisted_consumed: usize,
    /// Sidecar residue at the final audit attributed
    /// to injected lost/misdirected writes — recorded (never silently
    /// excused) and then REQUIRED to heal on one reopen (the
    /// reopen-heals contract, asserted on injected residue too).
    pub attributed_residue: Vec<String>,
    /// Every reconcile arbitration this universe performed —
    /// (context, verdict, channel) where context names what died or was
    /// deferred (`crash:<window>@op<i>`, `crash-state:write#k@op<i>`,
    /// `fault@op<i>`, `watch-interrupt@op<i>` for a watch-ending op judged
    /// inside the resolution, `keep-serving-deferred@op<i>`),
    /// verdict is `Applied` / `ForkOnly` / `NotApplied` — the
    /// keep-serving-deferred rows append ` matched=<composition>`
    /// provenance (e.g. `Applied matched=A+E`), a human-triage surface,
    /// deliberately unasserted (lance-realm compositions are
    /// process-context-sensitive) —
    /// and channel is the observation surface the ruling rested on
    /// ("query", or "query+physical" when the ghost tie-break consulted
    /// the physical channel). The per-death RESULT the ledger records for
    /// hits. Deterministic and replay-compared for adapter-realm one-op
    /// rows — an arbitration that flips between same-seed runs is itself
    /// a caught bug; the keep-serving rows carry the lance-realm envelope
    /// carve-out.
    pub reconcile_verdicts: Vec<(String, String, String)>,
    /// Known-defect encounters AND experiment bookkeeping this universe
    /// had — carve-outs and by-design behaviors firing during workload
    /// ops, each tagged with its tracking reference (e.g.
    /// `reopen-heals-barrier@op12`, `recovery-barrier-on-retry@op9`),
    /// plus the keep-serving rows: `keep-serving-defer@op<i>:<id>` (or
    /// `:recovery-barrier` for the clean-recovery-state spelling that
    /// names no id; consumed by the pinned panel's shape assert) and the
    /// resolution rows (`keep-serving-healed@/interrupted@/expired@end`,
    /// asserted defer-implies-resolution by the widened regression pin).
    pub known_issues: Vec<String>,
    /// bounded-staleness bite counters — content reads served
    /// as-of an earlier tick, listings served with as-of membership
    /// (0 without the staleness knobs). These count DELIVERED lies.
    pub stale_reads_served: usize,
    pub stale_lists_served: usize,
}

// ------------------------------------------------------------------- model --

#[derive(Clone, Debug, Default)]
struct Model {
    /// name → (age, ver); ver = -1 for rows written without one.
    persons: BTreeMap<String, (i64, i64)>,
    edges: BTreeSet<(String, String)>,
    /// The physical-vs-logical edge delta — EMPTY by construction since
    /// the #474 fix made self-loops ordinary visible edges. Kept (with its
    /// vestigial cascade/remove/fork/merge carries) so the physical-channel
    /// oracle keeps proving raw == logical ∪ ghosts, which now means
    /// raw == logical; anything entering this set again is a regression.
    ghosts: BTreeSet<(String, String)>,
}

impl Model {
    fn person_rows(&self) -> Vec<(String, i64, i64)> {
        self.persons
            .iter()
            .map(|(name, (age, ver))| (name.clone(), *age, *ver))
            .collect()
    }
    /// The raw-channel expectation: logical edges ∪ ghosts, sorted (BTreeSet
    /// union) — the one spelling of what `physical_view_on` must show.
    fn edges_with_ghosts(&self) -> Vec<(String, String)> {
        self.edges.union(&self.ghosts).cloned().collect()
    }
    fn edge_pairs(&self) -> Vec<(String, String)> {
        self.edges.iter().cloned().collect()
    }
    fn has_edges_touching(&self, name: &str) -> bool {
        self.edges
            .iter()
            .any(|(from, to)| from == name || to == name)
    }
}

// ------------------------------------------------- branch-aware world model --

/// Branch pool for the workload — fixed, small, clean names. Branch NAMING is
/// not the axis under test (hostile keys stay in data); a bounded pool keeps
/// the model's bookkeeping trivial while still cycling create/merge/delete.
const BRANCH_POOL: [&str; 2] = ["b0", "b1"];
/// Owned "main" for call sites needing `&[String]` (per-check
/// mode differential runs main-only; final audit covers every branch).
static MAIN_BRANCH: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| "main".to_string());

/// CORRUPTION AXIS (persisted tier) — FIRST-CONTACT FINDING (2026-08-13, seed 97's first
/// run) + its named carve-out: recovery LISTS and RE-READS a
/// foreign-named file in `__recovery/` (a misdirected sidecar, `dstm-`
/// prefix) but neither heals nor removes it — permanent residue, silently
/// re-consumed on every recovery pass. Issue candidate (Azim judges): what
/// is the contract for an unrecognized sidecar file — quarantine, delete,
/// or refuse? Until ruled, residue whose FILENAME carries our misdirect
/// marker (only `misdirect_uri` mints `dstm-`) is recorded as a
/// `s11b-foreign-sidecar-ignored` known-issue row instead of panicking;
/// REAL-named residue keeps panicking (reopen must heal what it
/// recognizes). Per-universe sink, cleared at universe start, drained into
/// `UniverseReport.known_issues` (lance_faults slot precedent).
static FOREIGN_SIDECAR_ROWS: Mutex<Vec<String>> = Mutex::new(Vec::new());

fn is_foreign_sidecar(uri: &str) -> bool {
    uri.rsplit_once('/')
        .map(|(_, file)| file.starts_with("dstm-"))
        .unwrap_or(false)
}

/// Partition residue: foreign-marked entries are recorded (root-normalized)
/// and returned as tolerated; anything else is returned for the caller to
/// panic on.
fn partition_residue(residue: Vec<String>, root: &str, label: &str) -> Vec<String> {
    let mut hard = Vec::new();
    for uri in residue {
        if is_foreign_sidecar(&uri) {
            FOREIGN_SIDECAR_ROWS.lock().unwrap().push(format!(
                "s11b-foreign-sidecar-ignored:{}@{label}",
                uri.replace(root, "<root>")
            ));
        } else {
            hard.push(uri);
        }
    }
    hard
}

#[derive(Clone, Debug)]
struct BranchSlot {
    state: Model,
    /// Main's state at `branch_create` time — the three-way merge base.
    base: Model,
    /// Merge-and-close workflow: a merged branch is never merged AGAIN (the
    /// engine's second-merge base is lineage-dependent; the model stays out
    /// of that ambiguity). It may still be mutated and is eventually deleted.
    merged: bool,
}

/// The differential model made branch-aware: main plus one replica per live
/// branch, each remembering its fork-point state as the merge base.
#[derive(Clone, Debug, Default)]
struct WorldModel {
    main: Model,
    branches: BTreeMap<String, BranchSlot>,
}

impl WorldModel {
    fn state_of(&self, branch: &str) -> &Model {
        self.state_of_opt(branch).expect("live branch")
    }
    /// Branch-absence-safe sibling of [`Self::state_of`]: `None` for a branch
    /// the model does not hold. Callers judging state that can lag or lead
    /// the model (a keep-serving ruling may have removed a branch between an
    /// op's sampling and its judgment) use this, never the panicking form.
    fn state_of_opt(&self, branch: &str) -> Option<&Model> {
        if branch == "main" {
            Some(&self.main)
        } else {
            self.branches.get(branch).map(|slot| &slot.state)
        }
    }
    fn state_of_mut(&mut self, branch: &str) -> &mut Model {
        if branch == "main" {
            &mut self.main
        } else {
            &mut self.branches.get_mut(branch).expect("live branch").state
        }
    }
    /// Deterministic observation order: main first, then branches by name.
    fn branch_names(&self) -> Vec<String> {
        let mut names = vec!["main".to_string()];
        names.extend(self.branches.keys().cloned());
        names
    }
    fn render(&self) -> WorldState {
        self.branch_names()
            .iter()
            .map(|name| {
                let m = self.state_of(name);
                (name.clone(), m.person_rows(), m.edge_pairs())
            })
            .collect()
    }
}

/// Predict the engine's three-way merge (exec/merge.rs `CandidateTableState`
/// cursor walk) at the LOGICAL-KEY level: per key compare base/source/target
/// by content; the unchanged side yields to the changed one; both-changed-
/// equal passes; both-changed-differently is a `MergeConflict` and the WHOLE
/// merge is rejected (state untouched). `None` = rejection predicted.
///
/// Two hypotheses layered on top (dual-hypothesis method, as with
/// recorded engine discoveries — if the engine disagrees, an assert fails loudly and
/// the real semantics get recorded with evidence):
///   H-A (EDGES ONLY since the 2026-08-14 predict-triage): an edge BORN on
///        both sides since the fork — the engine merges edges keyed on
///        ULID row id, never sees the two rows as one logical edge, and
///        ACCEPTS a duplicate (the born-on-both signature); the model keeps
///        predicting reject so the illegal state stays flagged. PERSON
///        rows are `@key`-keyed and the engine's measured semantics are
///        content-based: equal-content born-on-both CONVERGES to one row,
///        divergent inserts are typed `MergeConflict{DivergentInsert}` —
///        exactly the plain three-way arms, so persons skip H-A (probed
///        both cells, `dst_predict_born_on_both_person_probe`).
///   H-B: the merged state is RI-validated — an edge surviving the cursor
///        walk whose endpoint the other side deleted rejects the merge.
fn predict_merge(base: &Model, source: &Model, target: &Model) -> Option<Model> {
    // Predict-triage aid (env-gated): DST_PREDICT_LOG=1
    // prints WHICH rule rejected and on what evidence, so a
    // model-vs-engine disagreement names its own rule instead of just
    // its op.
    fn reject_log(rule: &str, detail: String) {
        if crate::harness::debug_knobs::PREDICT_LOG.load(std::sync::atomic::Ordering::Relaxed)
            || std::env::var("DST_PREDICT_LOG").is_ok()
        {
            println!("dst predict_merge REJECT [{rule}]: {detail}");
        }
    }
    fn three_way<K: Ord + Clone + std::fmt::Debug, V: Eq + Clone + std::fmt::Debug>(
        base: &BTreeMap<K, V>,
        source: &BTreeMap<K, V>,
        target: &BTreeMap<K, V>,
        // H-A applies to EDGES only — rationale in the fn doc's H-A entry.
        reject_born_on_both: bool,
    ) -> Option<BTreeMap<K, V>> {
        let mut keys: BTreeSet<&K> = BTreeSet::new();
        keys.extend(base.keys());
        keys.extend(source.keys());
        keys.extend(target.keys());
        let mut merged = BTreeMap::new();
        for key in keys {
            let b = base.get(key);
            let s = source.get(key);
            let t = target.get(key);
            if reject_born_on_both && b.is_none() && s.is_some() && t.is_some() {
                reject_log("H-A born-on-both", format!("key={key:?} s={s:?} t={t:?}"));
                return None; // H-A: two distinct fresh rows under one key
            }
            let pick = if s == b {
                t
            } else if t == b || s == t {
                s
            } else {
                reject_log(
                    "both-changed-differently",
                    format!("key={key:?} b={b:?} s={s:?} t={t:?}"),
                );
                return None; // both changed, differently → MergeConflict
            };
            if let Some(v) = pick {
                merged.insert(key.clone(), v.clone());
            }
        }
        Some(merged)
    }

    let persons = three_way(&base.persons, &source.persons, &target.persons, false)?;
    let to_map = |m: &Model| -> BTreeMap<(String, String), ()> {
        m.edges.iter().cloned().map(|p| (p, ())).collect()
    };
    let edges: BTreeSet<(String, String)> =
        three_way(&to_map(base), &to_map(source), &to_map(target), true)?
            .into_keys()
            .collect();
    for (from, to) in &edges {
        if !persons.contains_key(from) || !persons.contains_key(to) {
            reject_log("H-B referential", format!("edge=({from:?},{to:?})"));
            return None; // H-B: merged state referentially broken
        }
    }
    // Ghosts are carried by the CALLER (`apply_world`'s merge arm, via
    // `three_way_ghosts`) — predict_merge stays a purely logical judgment.
    Some(Model {
        persons,
        edges,
        ghosts: BTreeSet::new(),
    })
}

// --------------------------------------------------------------------- ops --

#[derive(Clone, Debug)]
enum Op {
    InsertV {
        name: String,
        age: i64,
        ver: i64,
    },
    UpdateV {
        name: String,
        age: i64,
        ver: i64,
    },
    DeletePerson {
        name: String,
    },
    AddFriend {
        from: String,
        to: String,
    },
    RemoveFriendshipsFrom {
        from: String,
    },
    InsertLegacy {
        name: String,
        age: i64,
    },
    /// Maintenance ops (a cheap translation of slatedb's background
    /// actors): they must NEVER change logical state — a new free invariant,
    /// since the model is untouched and continuous verification still holds.
    Optimize,
    Cleanup,
    EnsureIndices,
    /// The widened families (sampled only under `Scenario::wide`).
    /// Schema evolution is additive-only (extra optional Person props), so
    /// it joins the logically-invisible set the model ignores. The focused
    /// schema-add regression passes with Lance 11, but randomized schema-op
    /// requalification is deferred (see the roll-12 note). Keep it out of the
    /// sampler, with dead_code allowed, until that qualification is complete.
    #[allow(dead_code)]
    SchemaAddProperty {
        count: usize,
    },
    /// Mid-life bulk load, `LoadMode::Merge` (upsert by @key) over the load
    /// alphabet. Model: upsert each payload row.
    LoadMerge {
        people: Vec<(String, i64, i64)>,
    },
    /// Mid-life bulk load, `LoadMode::Append` (StrictInsert) — names are
    /// fresh-by-construction (`ld{n}` counter) so the insert can't collide.
    LoadAppend {
        people: Vec<(String, i64, i64)>,
    },
    /// `db.refresh()` — runs the roll-forward healer (the roll-forward-heal finding);
    /// logically invisible.
    Refresh,
    /// `db.sync_branch(branch)` — view sync; logically invisible.
    SyncBranch,
}

fn sample_op(rng: &mut SplitMix64, model: &Model, next_ver: &mut i64, hostile: bool) -> Op {
    let alphabet: &[&str] = if hostile { &HOSTILE } else { &NAMES };
    let name = alphabet[rng.below(alphabet.len() as u64) as usize].to_string();
    let age = rng.below(90) as i64;
    match rng.below(9) {
        0 => {
            *next_ver += 1;
            Op::InsertV {
                name,
                age,
                ver: *next_ver,
            }
        }
        1 => {
            *next_ver += 1;
            Op::UpdateV {
                name,
                age,
                ver: *next_ver,
            }
        }
        2 => Op::DeletePerson { name },
        3 => {
            // Endpoints drawn from the model's live population when possible.
            let keys: Vec<&String> = model.persons.keys().collect();
            if keys.len() < 2 {
                *next_ver += 1;
                return Op::InsertV {
                    name,
                    age,
                    ver: *next_ver,
                };
            }
            let from = keys[rng.below(keys.len() as u64) as usize].clone();
            let to = keys[rng.below(keys.len() as u64) as usize].clone();
            Op::AddFriend { from, to }
        }
        4 => Op::RemoveFriendshipsFrom { from: name },
        5 => Op::InsertLegacy { name, age },
        6 => Op::Optimize,
        7 => Op::Cleanup,
        _ => Op::EnsureIndices,
    }
}

async fn exec_op(db: &mut Omnigraph, branch: &str, op: &Op) -> OmniResult<()> {
    match op {
        Op::InsertV { name, age, ver } => mutate_on(
            db,
            branch,
            MUTATION_QUERIES,
            "insert_person_v",
            &mixed_params(&[("$name", name)], &[("$age", *age), ("$ver", *ver)]),
        )
        .await
        .map(|_| ()),
        Op::UpdateV { name, age, ver } => mutate_on(
            db,
            branch,
            MUTATION_QUERIES,
            "set_age_v",
            &mixed_params(&[("$name", name)], &[("$age", *age), ("$ver", *ver)]),
        )
        .await
        .map(|_| ()),
        Op::DeletePerson { name } => mutate_on(
            db,
            branch,
            MUTATION_QUERIES,
            "remove_person",
            &mixed_params(&[("$name", name)], &[]),
        )
        .await
        .map(|_| ()),
        Op::AddFriend { from, to } => mutate_on(
            db,
            branch,
            MUTATION_QUERIES,
            "add_friend",
            &mixed_params(&[("$from", from), ("$to", to)], &[]),
        )
        .await
        .map(|_| ()),
        Op::RemoveFriendshipsFrom { from } => mutate_on(
            db,
            branch,
            MUTATION_QUERIES,
            "remove_friendships_from",
            &mixed_params(&[("$from", from)], &[]),
        )
        .await
        .map(|_| ()),
        Op::InsertLegacy { name, age } => mutate_on(
            db,
            branch,
            MUTATION_QUERIES,
            "insert_person",
            &mixed_params(&[("$name", name)], &[("$age", *age)]),
        )
        .await
        .map(|_| ()),
        // Boxed: the engine's maintenance futures are enormous; inlining all
        // three into one poll fn overflows the 2 MiB test stack (known engine
        // trait — see lessons_learned on RUST_MIN_STACK).
        Op::Optimize => Box::pin(db.optimize()).await.map(|_| ()),
        Op::Cleanup => Box::pin(db.cleanup(omnigraph::db::CleanupPolicyOptions {
            keep_versions: Some(1),
            older_than: None,
        }))
        .await
        .map(|_| ()),
        Op::EnsureIndices => Box::pin(db.ensure_indices()).await.map(|_| ()),
        Op::SchemaAddProperty { count } => Box::pin(db.apply_schema(&schema_with_extras(*count)))
            .await
            .map(|_| ()),
        Op::LoadMerge { people } => {
            // A constant Company row rides along so the load stages TWO
            // tables (node:Person + node:Company) — otherwise
            // `load.between_table_stages` can never be crossed. Invisible to
            // the model by design (oracles observe persons + Knows edges);
            // Merge upserts it idempotently by @key.
            let payload = format!(
                "{}\n{{\"type\": \"Company\", \"data\": {{\"name\": \"lc0\"}}}}",
                person_jsonl(people)
            );
            Box::pin(load_jsonl(db, &payload, LoadMode::Merge))
                .await
                .map(|_| ())
        }
        Op::LoadAppend { people } => {
            // Two-table payload like LoadMerge, but Append (StrictInsert)
            // can't reuse a constant Company — derive a fresh one from the
            // op's own fresh person name (unique by construction).
            let payload = format!(
                "{}\n{{\"type\": \"Company\", \"data\": {{\"name\": \"c-{}\"}}}}",
                person_jsonl(people),
                people[0].0
            );
            Box::pin(load_jsonl(db, &payload, LoadMode::Append))
                .await
                .map(|_| ())
        }
        Op::Refresh => Box::pin(db.refresh()).await,
        // Always "main": non-main sync switches the handle's write base and
        // with it every later fork base / load target (see the sampler note).
        Op::SyncBranch => {
            let _ = branch;
            Box::pin(db.sync_branch("main")).await
        }
    }
}

/// Mirror one SUCCESSFUL op into the model. Delete cascades edges touching
/// the person (dual-hypothesis discovery: if the engine instead preserves or
/// forbids, continuous verification fails loudly on first contact and the
/// policy gets corrected with evidence in hand).
fn apply_to_model(model: &mut Model, op: &Op) {
    match op {
        Op::InsertV { name, age, ver } => {
            model.persons.insert(name.clone(), (*age, *ver));
        }
        Op::UpdateV { name, age, ver } => {
            if model.persons.contains_key(name) {
                model.persons.insert(name.clone(), (*age, *ver));
            }
        }
        Op::DeletePerson { name } => {
            model.persons.remove(name);
            model.edges.retain(|(from, to)| from != name && to != name);
            // Vestigial post-#474 (ghosts is empty by construction):
            model.ghosts.retain(|(from, to)| from != name && to != name);
        }
        Op::AddFriend { from, to } => {
            // Self-loops are ordinary visible edges since the #474 fix
            // (traversal emits the self-edge; it no longer enters the
            // frontier). The pre-fix ghost class — stored but hidden from
            // traversal — is dead; `ghosts` stays as the (now empty)
            // physical-vs-logical delta so the physical-channel oracle
            // still proves raw == logical by construction.
            model.edges.insert((from.clone(), to.clone()));
        }
        Op::RemoveFriendshipsFrom { from } => {
            model.edges.retain(|(f, _)| f != from);
            // Vestigial post-#474 (ghosts is empty by construction):
            model.ghosts.retain(|(f, _)| f != from);
        }
        Op::InsertLegacy { name, age } => {
            model.persons.insert(name.clone(), (*age, -1));
        }
        Op::LoadMerge { people } | Op::LoadAppend { people } => {
            for (name, age, ver) in people {
                model.persons.insert(name.clone(), (*age, *ver));
            }
        }
        // Maintenance is logically invisible by contract; schema evolution is
        // additive-only; refresh/sync only move the handle's view.
        Op::Optimize
        | Op::Cleanup
        | Op::EnsureIndices
        | Op::SchemaAddProperty { .. }
        | Op::Refresh
        | Op::SyncBranch => {}
    }
}

// --------------------------------------------------------------- world ops --

/// One workload step against the whole store: a data/maintenance op on a
/// chosen branch, or a branch-lifecycle verb. Merge target is always main
/// (the modeled workflow: fork from main, work, merge back, close).
#[derive(Clone, Debug)]
enum WorldOp {
    Data {
        branch: String,
        op: Op,
    },
    BranchCreate {
        name: String,
    },
    BranchMerge {
        source: String,
    },
    BranchDelete {
        name: String,
    },
    /// `load_as(branch, base=Some("main"), …)` with a missing
    /// target: the implicit fork-if-missing path (the fork-if-missing gap), the ONLY
    /// route to `load.post_branch_create_pre_stage` and the fork-survives state (a
    /// failed load's surviving empty branch).
    LoadFork {
        branch: String,
        people: Vec<(String, i64, i64)>,
    },
}

/// 12-sided sampler (16-sided under `wide`): rolls 9–11 are the branch verbs
/// (falling back to a data op when their precondition doesn't hold), rolls
/// 12–15 the loader-walk families (schema evolution / mid-life load / refresh
/// / sync), everything else the existing 9-op mix on a uniformly-sampled
/// live branch (main included).
#[allow(clippy::too_many_arguments)]
fn sample_world_op(
    rng: &mut SplitMix64,
    world: &WorldModel,
    next_ver: &mut i64,
    hostile: bool,
    wide: bool,
    schema_extras: &mut usize,
    fresh_load: &mut usize,
) -> WorldOp {
    let die = if wide { rng.below(16) } else { rng.below(12) };
    match die {
        // Roll 12 is the quarantined SchemaAddProperty slot. The original
        // poisoned-traversal sequence now passes with Lance 11, pinned by
        // `dst_schema_add_property_after_mutation_preserves_traversal`.
        // Randomized schema-op requalification is deferred: keep the load
        // frequency and RNG stream unchanged for the substrate cost comparison.
        // Re-enabling this slot and the schema_apply/schema_reload families in
        // `workload_can_reach` belong to that separate qualification.
        12 | 13 => {
            let _ = &schema_extras;
            // Loads run on main (`load_jsonl` targets the active branch)
            // except the fork flavor, which creates a free pool branch from
            // main via the implicit fork-if-missing path.
            let flavor = rng.below(3);
            if flavor == 1 {
                let mut people = Vec::new();
                for _ in 0..2 {
                    *fresh_load += 1;
                    *next_ver += 1;
                    people.push((
                        format!("ld{}", *fresh_load),
                        rng.below(90) as i64,
                        *next_ver,
                    ));
                }
                return WorldOp::Data {
                    branch: "main".to_string(),
                    op: Op::LoadAppend { people },
                };
            }
            // Distinct names by construction: the engine REJECTS a payload
            // carrying the same @key twice even in Merge mode (BadRequest
            // "@unique violation on Person.name: value 'l1' held by 'l1' and
            // 'l1'", observed 2026-08-11).
            let first = rng.below(LOAD_NAMES.len() as u64) as usize;
            let second =
                (first + 1 + rng.below((LOAD_NAMES.len() - 1) as u64) as usize) % LOAD_NAMES.len();
            let mut people = Vec::new();
            for idx in [first, second] {
                *next_ver += 1;
                people.push((LOAD_NAMES[idx].to_string(), rng.below(90) as i64, *next_ver));
            }
            let fork_slot = (flavor == 2)
                .then(|| {
                    BRANCH_POOL
                        .iter()
                        .find(|n| !world.branches.contains_key(**n))
                })
                .flatten();
            if let Some(name) = fork_slot {
                return WorldOp::LoadFork {
                    branch: name.to_string(),
                    people,
                };
            }
            return WorldOp::Data {
                branch: "main".to_string(),
                op: Op::LoadMerge { people },
            };
        }
        14 => {
            return WorldOp::Data {
                branch: "main".to_string(),
                op: Op::Refresh,
            };
        }
        15 => {
            // MAIN-ONLY by discovery (2026-08-11, first wide hunt universe):
            // `sync_branch` SWITCHES the handle's write base (documented:
            // "Synchronize this handle's write base to the latest head of
            // the named branch"), so a non-main sync silently redirects every
            // later `branch_create` fork base and `load_jsonl` target — the
            // world model caught engine-b1 == engine-b0 within one universe.
            // Modeling the active-branch state (per-branch fork parents) is a
            // deferred widening; until then the op pins to main, which still
            // exercises the coordinator-swap path.
            return WorldOp::Data {
                branch: "main".to_string(),
                op: Op::SyncBranch,
            };
        }
        _ => {}
    }
    match die {
        9 => {
            if let Some(name) = BRANCH_POOL
                .iter()
                .find(|n| !world.branches.contains_key(**n))
            {
                return WorldOp::BranchCreate {
                    name: name.to_string(),
                };
            }
        }
        10 => {
            let cands: Vec<&String> = world
                .branches
                .iter()
                .filter(|(_, slot)| !slot.merged)
                .map(|(name, _)| name)
                .collect();
            if !cands.is_empty() {
                let source = cands[rng.below(cands.len() as u64) as usize].clone();
                return WorldOp::BranchMerge { source };
            }
        }
        11 => {
            let cands: Vec<&String> = world.branches.keys().collect();
            if !cands.is_empty() {
                let name = cands[rng.below(cands.len() as u64) as usize].clone();
                return WorldOp::BranchDelete { name };
            }
        }
        _ => {}
    }
    let names = world.branch_names();
    let branch = names[rng.below(names.len() as u64) as usize].clone();
    let op = sample_op(rng, world.state_of(&branch), next_ver, hostile);
    WorldOp::Data { branch, op }
}

// ------------------------------------------ targeted scheduling --

/// Mutation-class data ops — everything that walks the mutation write path
/// (stage → effect gates → table commit → manifest publish).
fn is_mutation_op(op: &Op) -> bool {
    matches!(
        op,
        Op::InsertV { .. }
            | Op::UpdateV { .. }
            | Op::DeletePerson { .. }
            | Op::AddFriend { .. }
            | Op::RemoveFriendshipsFrom { .. }
            | Op::InsertLegacy { .. }
    )
}

/// Mid-life bulk-load ops — they route through the loader pipeline and the
/// shared `commit_all` machinery (the shared load/mutation boundary).
fn is_load_op(op: &Op) -> bool {
    matches!(op, Op::LoadMerge { .. } | Op::LoadAppend { .. })
}

/// Window family (failpoint name prefix) → the op kinds that can execute
/// through it. Static knowledge read off the catalog plus the v0/v1 hunts'
/// EVIDENCE: `mutation.*`, `graph_publish.*`, `publish.*`, `fork.*`,
/// `recovery.*` and `classify.*` all hit via mutation ops (the write path
/// consults recovery sidecars and fork classification at txn open); loads
/// share that machinery; refresh/sync run the roll-forward healer (loader
/// walk Finding 5); the maintenance and branch families are their own verbs.
/// Families the workload has NO op for return false everywhere (init/open →
/// schedule-scope).
fn window_matches(window: &str, wop: &WorldOp) -> bool {
    // Precision rules first: windows whose NAME pins them to one op kind
    // tighter than their family (scheduling a delete-only window on inserts
    // is a guaranteed miss).
    if window == "mutation.delete_node_pre_primary_delete" {
        return matches!(
            wop,
            WorldOp::Data {
                op: Op::DeletePerson { .. },
                ..
            }
        );
    }
    if window == "load.post_branch_create_pre_stage" {
        // Only the implicit fork-if-missing path crosses this one.
        return matches!(wop, WorldOp::LoadFork { .. });
    }
    if window == "mutation.post_sidecar_pre_fork" || window == "mutation.post_fork_pre_commit" {
        // Deferred-fork route: only a data op OFF main can be a first touch
        // (main's tables are native, never forked). Scheduling these on main
        // ops is a guaranteed miss.
        return matches!(
            wop,
            WorldOp::Data { branch, op, .. } if branch != "main" && is_mutation_op(op)
        );
    }
    if window == "ensure_indices.post_sidecar_pre_fork" {
        // Same deferred-fork gate, ensure_indices flavor.
        return matches!(
            wop,
            WorldOp::Data {
                branch,
                op: Op::EnsureIndices,
                ..
            } if branch != "main"
        );
    }
    let family = window.split('.').next().unwrap_or(window);
    match family {
        "branch_create" => matches!(wop, WorldOp::BranchCreate { .. } | WorldOp::LoadFork { .. }),
        "branch_merge" => matches!(wop, WorldOp::BranchMerge { .. }),
        "branch_delete" => matches!(wop, WorldOp::BranchDelete { .. }),
        "branch_control" => matches!(
            wop,
            WorldOp::BranchCreate { .. }
                | WorldOp::BranchMerge { .. }
                | WorldOp::BranchDelete { .. }
                | WorldOp::LoadFork { .. }
        ),
        "load" => matches!(
            wop,
            WorldOp::LoadFork { .. }
                | WorldOp::Data {
                    op: Op::LoadMerge { .. } | Op::LoadAppend { .. },
                    ..
                }
        ),
        "schema_apply" | "schema_reload" => matches!(
            wop,
            WorldOp::Data {
                op: Op::SchemaAddProperty { .. },
                ..
            }
        ),
        "optimize" => matches!(
            wop,
            WorldOp::Data {
                op: Op::Optimize,
                ..
            }
        ),
        "cleanup" => matches!(
            wop,
            WorldOp::Data {
                op: Op::Cleanup,
                ..
            }
        ),
        "ensure_indices" => matches!(
            wop,
            WorldOp::Data {
                op: Op::EnsureIndices,
                ..
            }
        ),
        "mutation" | "graph_publish" | "publish" | "fork" | "classify" => {
            matches!(wop, WorldOp::LoadFork { .. })
                || matches!(wop, WorldOp::Data { op, .. } if is_mutation_op(op) || is_load_op(op))
        }
        "recovery" => {
            matches!(wop, WorldOp::LoadFork { .. })
                || matches!(wop, WorldOp::Data { op, .. } if is_mutation_op(op)
                    || is_load_op(op)
                    || matches!(op, Op::Refresh | Op::SyncBranch))
        }
        _ => false,
    }
}

/// Does scheduling this window require the WIDE workload? The hunt uses the
/// narrow (12-die) workload everywhere else: the wide die dilutes branch-verb
/// frequency (1/16 vs 1/12 per verb, plus 4 diverting rolls), which measured
/// as ALL `branch_merge.*` windows going dark in the first all-wide hunt pass —
/// dilution costs more coverage than the new families add, so wide is scoped
/// to the windows only its ops can reach.
pub fn window_needs_wide(window: &str) -> bool {
    matches!(window.split('.').next().unwrap_or(window), "load")
}

/// Can the CURRENT workload produce any op reaching this window's family?
/// The hunt uses this to SKIP unschedulable windows instead of burning matrix
/// cells on them — the miss is reported as "unschedulable", not "never
/// reached", which keeps the coverage report honest about WHY a window
/// stayed dark.
/// Whitelist mirrors `window_matches` exactly.
pub fn workload_can_reach(window: &str) -> bool {
    matches!(
        window.split('.').next().unwrap_or(window),
        "branch_create"
            | "branch_merge"
            | "branch_delete"
            | "branch_control"
            | "optimize"
            | "cleanup"
            | "ensure_indices"
            // schema_apply/schema_reload: the focused regression passes with
            // Lance 11, but randomized schema-op requalification is deferred.
            // Keep these families absent while the sampler excludes the op.
            | "load"
            | "mutation"
            | "graph_publish"
            | "publish"
            | "fork"
            | "recovery"
            | "classify"
    )
}

async fn exec_world_op(db: &mut Omnigraph, wop: &WorldOp) -> OmniResult<()> {
    match wop {
        WorldOp::Data { branch, op } => exec_op(db, branch, op).await,
        WorldOp::BranchCreate { name } => Box::pin(db.branch_create(name)).await,
        // Boxed like the maintenance futures: the merge/delete state machines
        // are deep (2 MiB test stack).
        WorldOp::BranchMerge { source } => {
            Box::pin(db.branch_merge(source, "main")).await.map(|_| ())
        }
        WorldOp::BranchDelete { name } => Box::pin(db.branch_delete(name)).await,
        WorldOp::LoadFork { branch, people } => {
            // Same two-table payload trick as LoadMerge (see there).
            let payload = format!(
                "{}\n{{\"type\": \"Company\", \"data\": {{\"name\": \"lc0\"}}}}",
                person_jsonl(people)
            );
            Box::pin(db.load_as(branch, Some("main"), &payload, LoadMode::Merge, None))
                .await
                .map(|_| ())
        }
    }
}

/// Mirror one SUCCESSFUL world op into the model. A merge the model predicts
/// as conflicting is a no-op here — the engine rejects it wholesale, so the
/// caller records it as a legal rejection (and asserts the engine actually
/// DID reject; see the success-path assert in `run_universe`).
fn apply_world(world: &mut WorldModel, wop: &WorldOp) {
    match wop {
        WorldOp::Data { branch, op } => apply_to_model(world.state_of_mut(branch), op),
        WorldOp::BranchCreate { name } => {
            world.branches.insert(
                name.clone(),
                BranchSlot {
                    state: world.main.clone(),
                    base: world.main.clone(),
                    merged: false,
                },
            );
        }
        WorldOp::BranchMerge { source } => {
            let slot = &world.branches[source.as_str()];
            if let Some(mut merged) = predict_merge(&slot.base, &slot.state, &world.main) {
                // H: ghost rows ride merges like ordinary rows
                // (set-level three-way; a bare (X,X) pair has no conflict
                // shape at set level). predict_merge builds the merged model
                // from logical state only, so the ghost set is carried here.
                merged.ghosts =
                    three_way_ghosts(&slot.base.ghosts, &slot.state.ghosts, &world.main.ghosts);
                world.main = merged;
                world
                    .branches
                    .get_mut(source.as_str())
                    .expect("live branch")
                    .merged = true;
            }
        }
        WorldOp::BranchDelete { name } => {
            world.branches.remove(name.as_str());
        }
        WorldOp::LoadFork { branch, people } => {
            let mut state = world.main.clone();
            for (name, age, ver) in people {
                state.persons.insert(name.clone(), (*age, *ver));
            }
            world.branches.insert(
                branch.clone(),
                BranchSlot {
                    state,
                    base: world.main.clone(),
                    merged: false,
                },
            );
        }
    }
}

/// Apply the fork-survives half-state (see `ReconcileOutcome::ForkOnly`).
fn apply_fork_only(world: &mut WorldModel, wop: &WorldOp) {
    if let WorldOp::LoadFork { branch, .. } = wop {
        world.branches.insert(
            branch.clone(),
            BranchSlot {
                state: world.main.clone(),
                base: world.main.clone(),
                merged: false,
            },
        );
    }
}

/// set-level three-way for the ghost set: a pair survives the
/// merge iff added on either side, or present at base and removed on
/// neither. (Changed side wins; both-added is idempotent at set level.)
fn three_way_ghosts(
    base: &BTreeSet<(String, String)>,
    source: &BTreeSet<(String, String)>,
    target: &BTreeSet<(String, String)>,
) -> BTreeSet<(String, String)> {
    let mut all: BTreeSet<&(String, String)> = BTreeSet::new();
    all.extend(base);
    all.extend(source);
    all.extend(target);
    all.into_iter()
        .filter(|g| {
            let (b, s, t) = (base.contains(*g), source.contains(*g), target.contains(*g));
            if b { s && t } else { s || t }
        })
        .cloned()
        .collect()
}

/// Does the model expect the engine to reject this op as a merge conflict?
fn expects_merge_conflict(world: &WorldModel, wop: &WorldOp) -> bool {
    match wop {
        // `get`, not the panicking index: a keep-serving ruling can remove
        // the source branch between the op's sampling and a post-ruling
        // recompute. A merge on an absent branch predicts no conflict — its
        // refusal is legalized by `is_legal_rejection`'s dead-target member.
        WorldOp::BranchMerge { source } => world
            .branches
            .get(source.as_str())
            .map(|slot| predict_merge(&slot.base, &slot.state, &world.main).is_none())
            .unwrap_or(false),
        _ => false,
    }
}

fn is_merge_conflict_err(err: &OmniError) -> bool {
    format!("{err:?}").contains("MergeConflict")
}

/// THE REOPEN-HEALS DISCOVERY (targeted-scheduling hunt, 2026-08-10): a
/// Phase-D sidecar-delete failure inside a mutation is SWALLOWED by design
/// (recovery.rs `delete_sidecar`: "callers swallow it — the write already
/// published; the stale sidecar is healed by the next write or open"), so a
/// stale-but-confirmed sidecar can exist while the graph is healthy and the
/// mutation reports SUCCESS. `optimize`/`cleanup` conservatively refuse on ANY
/// sidecar ("requires a clean recovery state") because they cannot cheaply
/// tell stale-confirmed from partial. That refusal is therefore a LEGAL
/// rejection; the harness answers it like a real client — reopen (the
/// documented heal) via the reconcile path. Repro pinned:
/// `dst_discovery5_stale_sidecar_blocks_maintenance_until_reopen`.
/// The engine's second spelling of a pending-sidecar refusal — the
/// `manifest_conflict` text optimize/cleanup/schema-apply raise instead of
/// typed `RecoveryRequired`. One spelling: the keep-serving barrier branch
/// and [`is_recovery_barrier_rejection`] both key on it.
const CLEAN_RECOVERY_BARRIER_TEXT: &str = "requires a clean recovery state";

fn is_recovery_barrier_rejection(wop: &WorldOp, err: &OmniError) -> bool {
    matches!(
        wop,
        WorldOp::Data {
            op: Op::Optimize | Op::Cleanup | Op::SchemaAddProperty { .. },
            ..
        }
    ) && format!("{err:?}").contains(CLEAN_RECOVERY_BARRIER_TEXT)
}

// ------------------------------------------------------------------ faults --

/// Storage wrapper injecting the full `FaultPlan` at the adapter seam:
/// marked errors (so the workload can classify them as legal rejections)
/// and VIRTUAL-time latency on both read- and write-class calls, plus the
/// corruption (read + persisted tiers) and bounded-staleness axes.
#[derive(Debug)]
struct FailingStorage {
    inner: Arc<dyn StorageAdapter>,
    rng: Mutex<SplitMix64>,
    plan: FaultPlan,
    /// Faults apply only once enabled — init and fixture load stay clean so
    /// every universe starts from the same healthy world.
    enabled: std::sync::atomic::AtomicBool,
    /// Oracle windows suspend faults: verification measures ENGINE state,
    /// not the checker's own fault tolerance.
    suspended: std::sync::atomic::AtomicBool,
    /// the Lance-realm injector rides the same gates — every
    /// enable/suspend/resume call site toggles both realms at once.
    lance: Option<Arc<crate::lance_faults::LanceFaultState>>,
    /// the crash-state switch rides the same gates too, and its
    /// adapter-realm write/read hooks live in `write_fault`/`read_fault`.
    kill: Option<Arc<KillState>>,
    /// Feeds `UniverseReport::acks_lost`.
    acks_lost: std::sync::atomic::AtomicUsize,
    /// the DAMAGE LEDGER's read-tier form: URIs poisoned by
    /// latent sector errors (persistent, location-indexed), plus delivery
    /// counters. The ledger serves attribution (an op failure is attributed
    /// to corruption iff `damage_events()` advanced during the op) and the
    /// crossing story (delivered lies, not injected intent).
    poisoned: Mutex<BTreeSet<String>>,
    reads_corrupted: std::sync::atomic::AtomicUsize,
    reads_truncated: std::sync::atomic::AtomicUsize,
    latent_errors: std::sync::atomic::AtomicUsize,
    /// CORRUPTION AXIS (persisted tier) — the PERSISTED damage ledger: uri → verb for
    /// every stored lie (corrupt-write / lost-write / lost-delete /
    /// misdirect-source / misdirect-target). Read methods consult it to
    /// count CONSUMPTION (a read of a damaged URI — the bytes flow even
    /// under suspension, so consumption counts regardless of gates); the
    /// final audit consults it to attribute residue.
    persisted_damage: Mutex<std::collections::BTreeMap<String, &'static str>>,
    writes_corrupted: std::sync::atomic::AtomicUsize,
    writes_lost: std::sync::atomic::AtomicUsize,
    writes_misdirected: std::sync::atomic::AtomicUsize,
    persisted_consumed: std::sync::atomic::AtomicUsize,
    /// the staleness clock and memory. `staleness_tick`
    /// advances on every LANDED write-class call (count-landed-only, the
    /// kill enumerator's lesson); `key_history` keeps, per URI, the
    /// (tick, content, version-token) states the store has moved through —
    /// entries with content None are deletes, the base entry (tick 0)
    /// captures the pre-armed value so "as of before my first write" is
    /// answerable. Recorded only when a staleness knob is on. Serving rule:
    /// state as of tick T−k; a key with NO entry ≤ the as-of tick serves
    /// FRESH (world before the wrapper's knowledge is unknowable, and lying
    /// about it would be unbounded).
    staleness_tick: std::sync::atomic::AtomicU64,
    #[allow(clippy::type_complexity)] // per-key (tick, content, token) history, as documented
    key_history:
        Mutex<std::collections::BTreeMap<String, Vec<(u64, Option<String>, Option<String>)>>>,
    stale_reads_served: std::sync::atomic::AtomicUsize,
    stale_lists_served: std::sync::atomic::AtomicUsize,
}

/// CORRUPTION AXIS (persisted tier) — what `write_fault` decided about a write-class
/// call: proceed to the inner store, or the write is LOST (fabricate
/// success, no delegation, not kill-counted — it never reached the store).
enum WriteFate {
    Proceed,
    Lost,
}

impl FailingStorage {
    fn new(
        inner: Arc<dyn StorageAdapter>,
        plan: FaultPlan,
        lance: Option<Arc<crate::lance_faults::LanceFaultState>>,
        kill: Option<Arc<KillState>>,
    ) -> Self {
        Self {
            inner,
            rng: Mutex::new(SplitMix64(plan.seed)),
            plan,
            enabled: std::sync::atomic::AtomicBool::new(false),
            suspended: std::sync::atomic::AtomicBool::new(false),
            lance,
            kill,
            acks_lost: std::sync::atomic::AtomicUsize::new(0),
            poisoned: Mutex::new(BTreeSet::new()),
            reads_corrupted: std::sync::atomic::AtomicUsize::new(0),
            reads_truncated: std::sync::atomic::AtomicUsize::new(0),
            latent_errors: std::sync::atomic::AtomicUsize::new(0),
            persisted_damage: Mutex::new(std::collections::BTreeMap::new()),
            writes_corrupted: std::sync::atomic::AtomicUsize::new(0),
            writes_lost: std::sync::atomic::AtomicUsize::new(0),
            writes_misdirected: std::sync::atomic::AtomicUsize::new(0),
            persisted_consumed: std::sync::atomic::AtomicUsize::new(0),
            staleness_tick: std::sync::atomic::AtomicU64::new(0),
            key_history: Mutex::new(std::collections::BTreeMap::new()),
            stale_reads_served: std::sync::atomic::AtomicUsize::new(0),
            stale_lists_served: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn acks_lost(&self) -> usize {
        self.acks_lost.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reads_corrupted(&self) -> usize {
        self.reads_corrupted
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn reads_truncated(&self) -> usize {
        self.reads_truncated
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn latent_errors(&self) -> usize {
        self.latent_errors.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn writes_corrupted(&self) -> usize {
        self.writes_corrupted
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn writes_lost(&self) -> usize {
        self.writes_lost.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn writes_misdirected(&self) -> usize {
        self.writes_misdirected
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn persisted_consumed(&self) -> usize {
        self.persisted_consumed
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn stale_reads_count(&self) -> usize {
        self.stale_reads_served
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    fn stale_lists_count(&self) -> usize {
        self.stale_lists_served
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// total delivered damage events, the attribution counter:
    /// the universe loop snapshots this before each op; an op failure with
    /// the counter advanced is an attributed detection. Monotone. The persisted tier
    /// widens it: write-time injections AND consumptions of persisted
    /// damage count (a failure while an unrelated sidecar write was lost
    /// over-attributes mildly — recorded, never hidden).
    fn damage_events(&self) -> usize {
        self.reads_corrupted()
            + self.reads_truncated()
            + self.latent_errors()
            + self.writes_corrupted()
            + self.writes_lost()
            + self.writes_misdirected()
            + self.persisted_consumed()
    }

    /// CORRUPTION AXIS (persisted tier) — the residue-attribution view: URIs whose
    /// stored object is damaged-or-absent by INJECTION (lost/misdirected
    /// writes and lost deletes). The final audit partitions sidecar residue
    /// against this set.
    fn persisted_damage_snapshot(&self) -> std::collections::BTreeMap<String, &'static str> {
        self.persisted_damage.lock().unwrap().clone()
    }

    fn record_persisted(&self, uri: &str, verb: &'static str) {
        self.persisted_damage
            .lock()
            .unwrap()
            .insert(uri.to_string(), verb);
    }

    /// CORRUPTION AXIS (persisted tier) — consumption tracking: a read touching a URI in
    /// the persisted ledger consumed damaged (or injected-absent) state.
    /// Counts regardless of the fault gates — suspension stops CALL-PATH
    /// faults, but stored damage flows through any read. No draws, no
    /// behavior change; zero-knob plans have an empty ledger.
    fn note_persisted_read(&self, op: &str, uri: &str) {
        let verb = { self.persisted_damage.lock().unwrap().get(uri).copied() };
        if let Some(verb) = verb {
            self.persisted_consumed
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            println!("dst s11 damage-consumed: {verb} {op} {uri}");
        }
    }

    // ---------------------------------------- bounded staleness ---------

    fn staleness_on(&self) -> bool {
        self.plan.stale_read_pct > 0 || self.plan.stale_list_pct > 0
    }

    /// Capture a key's PRE-ARMED state as the tick-0 base entry the first
    /// time the staleness memory meets it (before its first tracked write),
    /// so "as of before my first write" is answerable with truth instead of
    /// an unbounded lie. Reads the inner store directly — never faulted.
    async fn staleness_base(&self, uri: &str) {
        if !self.staleness_on() {
            return;
        }
        let known = { self.key_history.lock().unwrap().contains_key(uri) };
        if known {
            return;
        }
        let base = match self.inner.read_text_versioned(uri).await {
            Ok((content, token)) => (0u64, Some(content), Some(token)),
            Err(_) => (0u64, None, None),
        };
        self.key_history
            .lock()
            .unwrap()
            .entry(uri.to_string())
            .or_insert_with(|| vec![base]);
    }

    /// Record a landed content write (or delete, content=None) at the next
    /// tick. Count-landed-only: callers invoke this AFTER successful
    /// delegation. History rings are capped; the base entry never rotates
    /// out (truthful floor).
    fn staleness_record(&self, uri: &str, content: Option<String>, token: Option<String>) {
        if !self.staleness_on() {
            return;
        }
        let tick = 1 + self
            .staleness_tick
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut map = self.key_history.lock().unwrap();
        let entries = map.entry(uri.to_string()).or_default();
        entries.push((tick, content, token));
        let cap = self.plan.max_lag_ticks.saturating_add(2).min(64) as usize;
        if entries.len() > cap + 1 {
            // keep the base (index 0) + the newest `cap` entries
            let drop_from = 1;
            let drop_count = entries.len() - cap - 1;
            entries.drain(drop_from..drop_from + drop_count);
        }
    }

    /// Roll one staleness decision for an eligible call. Same gates as every
    /// call-path fault (suspension keeps oracle reads truthful); draws from
    /// the plan rng only when the knob is nonzero.
    fn roll_stale(&self, pct: u64) -> Option<u64> {
        if pct == 0 || !self.active() {
            return None;
        }
        let (roll, lag) = {
            let mut rng = self.rng.lock().unwrap();
            (
                rng.below(100),
                1 + rng.below(self.plan.max_lag_ticks.max(1)),
            )
        };
        if roll >= pct {
            return None;
        }
        let now = self
            .staleness_tick
            .load(std::sync::atomic::Ordering::SeqCst);
        Some(now.saturating_sub(lag))
    }

    /// The key's state as of a tick: the last entry at-or-before it.
    /// `None` = the memory has no knowledge that old (pre-armed world) —
    /// callers serve FRESH. `Some((content, token))` with content `None`
    /// = the key did not exist at that tick (stale absence / it had been
    /// deleted); content `Some` may be a ZOMBIE if the head has since
    /// deleted the key.
    fn state_as_of(&self, uri: &str, as_of: u64) -> Option<(Option<String>, Option<String>)> {
        let map = self.key_history.lock().unwrap();
        let entries = map.get(uri)?;
        entries
            .iter()
            .rev()
            .find(|(t, _, _)| *t <= as_of)
            .map(|(_, c, v)| (c.clone(), v.clone()))
    }

    fn count_stale_read(&self, op: &str, uri: &str, as_of: u64) {
        self.stale_reads_served
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        println!("dst s25 stale: {op} {uri} as-of={as_of}");
    }

    /// CORRUPTION AXIS (persisted tier) — write-time corruption hook (pre-delegation):
    /// returns the (possibly mutated) contents to store. Persisted lie —
    /// recorded in the ledger, printed to the damage log.
    fn maybe_corrupt_write(&self, op: &str, uri: &str, contents: &str) -> Option<String> {
        if self.plan.corrupt_write_pct == 0 || !self.active() {
            return None;
        }
        let roll = { self.rng.lock().unwrap().below(100) };
        if roll >= self.plan.corrupt_write_pct {
            return None;
        }
        let pos = {
            let mut rng = self.rng.lock().unwrap();
            rng.below(contents.chars().count().max(1) as u64)
        };
        let rotted = bit_rot_text(contents, pos)?;
        self.writes_corrupted
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.record_persisted(uri, "corrupt-write");
        println!("dst s11 damage: corrupt-write@{pos} {op} {uri}");
        Some(rotted)
    }

    /// CORRUPTION AXIS (persisted tier) — misdirected-write hook: returns the WRONG
    /// target URI to write to (`misdirect_uri`). Ledger records BOTH halves of the damage: the
    /// intended object is absent (misdirect-source), the foreign object
    /// exists (misdirect-target).
    fn maybe_misdirect(&self, op: &str, uri: &str) -> Option<String> {
        if self.plan.misdirect_write_pct == 0 || !self.active() {
            return None;
        }
        let roll = { self.rng.lock().unwrap().below(100) };
        if roll >= self.plan.misdirect_write_pct {
            return None;
        }
        let target = misdirect_uri(uri);
        self.writes_misdirected
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.record_persisted(uri, "misdirect-source");
        self.record_persisted(&target, "misdirect-target");
        // The census recorded the ORIGINAL uri at write_fault entry; the
        // store gains the redirected key — record it too, so a censused
        // misdirect universe reconciles instead of flagging its own
        // injected damage as a bypass write.
        crate::write_census::record("adapter", op, &target, self.active());
        println!("dst s11 damage: misdirect {op} {uri} -> {target}");
        Some(target)
    }

    /// latent sector check, called AFTER the per-call fault
    /// rolls on content-read methods only (a poisoned "sector" underlies an
    /// object's bytes; listings and existence probes are metadata, and
    /// corrupting THOSE is the consistency territory). Membership
    /// first (no draw — persistence must not consume stream position), then
    /// a seeded poisoning roll. Suspension bypasses both: the poison lives
    /// in the call path, not the stored bytes, so oracle reads stay clean.
    fn latent_fault(&self, op: &str, uri: &str) -> OmniResult<()> {
        if self.plan.latent_read_pct == 0 || !self.active() {
            return Ok(());
        }
        if self.poisoned.lock().unwrap().contains(uri) {
            self.latent_errors
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(OmniError::manifest(format!(
                "{LATENT_MARKER}: post-poison {op} {uri}"
            )));
        }
        let roll = { self.rng.lock().unwrap().below(100) };
        if roll < self.plan.latent_read_pct {
            self.poisoned.lock().unwrap().insert(uri.to_string());
            self.latent_errors
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            println!("dst s11 damage: latent-poison {op} {uri}");
            return Err(OmniError::manifest(format!("{LATENT_MARKER}: {op} {uri}")));
        }
        Ok(())
    }

    /// the post-delegation LIE hook for content reads: seeded
    /// read-time bit rot (one substituted char) and/or truncation (strict
    /// prefix), applied to the text the inner store truthfully returned.
    /// No error, no marker — the caller cannot know. Each knob draws only
    /// when nonzero; the position draw happens only on a hit. Every
    /// delivered lie prints a `dst s11 damage` line (the damage-location
    /// log — the misdirect double-check needs to SEE which artifact class each
    /// lie landed on to judge whether green is earned or structural).
    fn maybe_corrupt(&self, op: &str, uri: &str, text: String) -> String {
        if !self.active() {
            return text;
        }
        let mut text = text;
        if self.plan.corrupt_read_pct > 0 {
            let roll = { self.rng.lock().unwrap().below(100) };
            if roll < self.plan.corrupt_read_pct {
                let pos = {
                    let mut rng = self.rng.lock().unwrap();
                    rng.below(text.chars().count().max(1) as u64)
                };
                if let Some(rotted) = bit_rot_text(&text, pos) {
                    text = rotted;
                    self.reads_corrupted
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    println!("dst s11 damage: bitrot@{pos} {op} {uri}");
                }
            }
        }
        if self.plan.truncate_read_pct > 0 {
            let roll = { self.rng.lock().unwrap().below(100) };
            if roll < self.plan.truncate_read_pct {
                let pos = {
                    let mut rng = self.rng.lock().unwrap();
                    rng.below(text.chars().count().max(1) as u64)
                };
                if let Some(cut) = truncate_text(&text, pos) {
                    text = cut;
                    self.reads_truncated
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    println!("dst s11 damage: truncate@{pos} {op} {uri}");
                }
            }
        }
        text
    }

    /// the ack-loss hook, called AFTER delegation on every
    /// write-class method: the position IS the semantics. `write_fault`
    /// (pre-delegation) can only model "the write never happened"; this
    /// hook receives the inner store's SUCCESS and converts it to a marked
    /// error — the effect is durable, only the acknowledgement is lost
    /// (the shape a dropped S3 200 produces). The dropped `T` (a CAS
    /// version, an if-absent verdict) models the response bytes that never
    /// arrived.
    async fn lose_ack<T>(&self, op: &str, uri: &str, out: OmniResult<T>) -> OmniResult<T> {
        if self.plan.ack_loss_pct == 0 || !self.active() {
            return out;
        }
        let Ok(value) = out else {
            return out;
        };
        let roll = { self.rng.lock().unwrap().below(100) };
        if roll < self.plan.ack_loss_pct {
            self.acks_lost
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let _ = value;
            return Err(OmniError::manifest(format!(
                "{ACK_LOSS_MARKER}: {op} {uri}"
            )));
        }
        Ok(value)
    }

    fn enable(&self) {
        self.enabled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(l) = &self.lance {
            l.enable();
        }
        if let Some(k) = &self.kill {
            k.enable();
        }
    }

    fn suspend(&self) {
        self.suspended
            .store(true, std::sync::atomic::Ordering::SeqCst);
        if let Some(l) = &self.lance {
            l.suspend();
        }
        if let Some(k) = &self.kill {
            k.suspend();
        }
    }

    fn resume(&self) {
        self.suspended
            .store(false, std::sync::atomic::Ordering::SeqCst);
        if let Some(l) = &self.lance {
            l.resume();
        }
        if let Some(k) = &self.kill {
            k.resume();
        }
    }

    fn active(&self) -> bool {
        self.enabled.load(std::sync::atomic::Ordering::SeqCst)
            && !self.suspended.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// COMPLETION-CUT hook (real lane B only; no-op otherwise): count a
    /// successful durable completion AFTER the inner store confirmed it.
    /// Known ordinal imprecisions, TODO(#527): both wait on the v2
    /// per-stage sequencer below the adapter — delete_prefix and
    /// rename_text count ONE completion though they expand into several
    /// lower stages; and the adapter maps a NotFound delete to Ok, so a
    /// no-op delete counts as a completion.
    fn count_completion(&self, op: &str, uri: &str) {
        if let Some(k) = &self.kill {
            k.on_completion(op, uri);
        }
    }

    async fn read_fault(&self, op: &str, uri: &str) -> OmniResult<()> {
        if let Some(k) = &self.kill
            && let Err(msg) = k.refuse_if_dead(op, uri)
        {
            return Err(OmniError::manifest(msg));
        }
        if !self.active() {
            return Ok(());
        }
        let (err_roll, lat_roll, lat_ms) = {
            let mut rng = self.rng.lock().unwrap();
            (
                rng.below(100),
                rng.below(100),
                1 + rng.below(self.plan.max_latency_ms.max(1)),
            )
        };
        if lat_roll < self.plan.latency_pct {
            tokio::time::sleep(std::time::Duration::from_millis(lat_ms)).await;
        }
        if err_roll < self.plan.read_error_pct {
            return Err(OmniError::manifest(format!("{FAULT_MARKER}: {op} {uri}")));
        }
        Ok(())
    }

    /// Death first (a dead process performs nothing), fault rolls second,
    /// the persisted-tier LOSE roll third, the kill COUNT last — a fault-rejected
    /// call never reaches the store, and neither does a LOST write, so
    /// neither is a crash-distinguishable durable write (count-landed-
    /// writes-only, audit improvement 2026-08-12; extended for lost writes
    /// 08-13). `lose_eligible` is false for the CAS (see the knob's doc).
    async fn write_fault(&self, op: &str, uri: &str, lose_eligible: bool) -> OmniResult<WriteFate> {
        if let Some(k) = &self.kill
            && let Err(msg) = k.refuse_if_dead(op, uri)
        {
            return Err(OmniError::manifest(msg));
        }
        // Census after the dead-check (post-mortem refusals are not
        // writes), mirroring the Lance-realm hook's placement. Counting
        // authority: this realm tags rows via `active()`, the Lance realm
        // via `KillState::counting()` — they agree because `enable()`
        // fans out to the kill state, so the census's counting==W
        // identity requires a kill state armed (the count-only probe).
        crate::write_census::record("adapter", op, uri, self.active());
        if !self.active() {
            return Ok(WriteFate::Proceed);
        }
        let (err_roll, lat_roll, lat_ms) = {
            let mut rng = self.rng.lock().unwrap();
            (
                rng.below(100),
                rng.below(100),
                1 + rng.below(self.plan.max_latency_ms.max(1)),
            )
        };
        if lat_roll < self.plan.latency_pct {
            tokio::time::sleep(std::time::Duration::from_millis(lat_ms)).await;
        }
        if err_roll < self.plan.error_pct {
            return Err(OmniError::manifest(format!("{FAULT_MARKER}: {op} {uri}")));
        }
        if lose_eligible && self.plan.lose_write_pct > 0 {
            let roll = { self.rng.lock().unwrap().below(100) };
            if roll < self.plan.lose_write_pct {
                self.writes_lost
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let verb = if op.starts_with("delete") {
                    "lost-delete"
                } else {
                    "lost-write"
                };
                self.record_persisted(uri, verb);
                println!("dst s11 damage: {verb} {op} {uri}");
                return Ok(WriteFate::Lost);
            }
        }
        if let Some(k) = &self.kill
            && let Err(msg) = k.on_write(op, uri)
        {
            return Err(OmniError::manifest(msg));
        }
        Ok(WriteFate::Proceed)
    }
}

/// CORRUPTION AXIS (persisted tier) — pure misdirection transform: same directory,
/// `dstm-` filename prefix (extension preserved) — the write lands at a
/// wrong key inside the same keyspace, so listings still see it.
pub(crate) fn misdirect_uri(uri: &str) -> String {
    match uri.rsplit_once('/') {
        Some((dir, file)) => format!("{dir}/dstm-{file}"),
        None => format!("dstm-{uri}"),
    }
}

/// pure, seeded read-time bit rot: substitute exactly one char
/// (index = `pos_roll`, already reduced modulo the char count by the caller's
/// draw). DIGIT-AWARE: a digit becomes a DIFFERENT
/// digit — a VALUE lie that survives every syntax check (the class a real
/// flipped byte produces when it lands in a numeric field, and the only
/// lie that can probe the silent tier); any other char becomes `'#'`
/// (syntax-hostile, exercising the detected tier), or `'%'` when it
/// already IS `'#'`, so the mutation is guaranteed to change the text.
/// Char-indexed, never byte-indexed: the adapter realm's content is UTF-8
/// `String`s and a mid-codepoint flip would be the HARNESS panicking, not
/// the store lying. `None` for empty text (nothing to rot — the caller
/// records no damage event).
pub(crate) fn bit_rot_text(text: &str, pos_roll: u64) -> Option<String> {
    let count = text.chars().count();
    if count == 0 {
        return None;
    }
    let idx = (pos_roll as usize) % count;
    Some(
        text.chars()
            .enumerate()
            .map(|(i, ch)| {
                if i == idx {
                    match ch.to_digit(10) {
                        Some(d) => char::from_digit((d + 1) % 10, 10).expect("digit"),
                        None if ch == '#' => '%',
                        None => '#',
                    }
                } else {
                    ch
                }
            })
            .collect(),
    )
}

/// pure, seeded truncation: keep a STRICT char prefix
/// (`pos_roll % count` chars — always at least one char shorter), the
/// partial-read-presented-as-complete lie. `None` for empty text.
pub(crate) fn truncate_text(text: &str, pos_roll: u64) -> Option<String> {
    let count = text.chars().count();
    if count == 0 {
        return None;
    }
    let keep = (pos_roll as usize) % count;
    Some(text.chars().take(keep).collect())
}

#[async_trait::async_trait]
impl StorageAdapter for FailingStorage {
    async fn read_text(&self, uri: &str) -> OmniResult<String> {
        self.read_fault("read_text", uri).await?;
        self.latent_fault("read_text", uri)?;
        self.note_persisted_read("read_text", uri);
        // a stale roll serves the as-of value (possibly a zombie
        // — head deleted, old value served). Stale ABSENCE falls through to
        // fresh on this method (no error fabrication in v1).
        if let Some(as_of) = self.roll_stale(self.plan.stale_read_pct)
            && let Some((Some(text), _)) = self.state_as_of(uri, as_of)
        {
            self.count_stale_read("read_text", uri, as_of);
            return Ok(self.maybe_corrupt("read_text", uri, text));
        }
        let out = self.inner.read_text(uri).await?;
        Ok(self.maybe_corrupt("read_text", uri, out))
    }
    async fn read_text_if_exists(&self, uri: &str) -> OmniResult<Option<String>> {
        self.read_fault("read_text_if_exists", uri).await?;
        self.latent_fault("read_text_if_exists", uri)?;
        self.note_persisted_read("read_text_if_exists", uri);
        // both polarities are legal lies here — an old value
        // (possibly a zombie) or a stale absence (`None` before the key's
        // creation reached "the replica").
        if let Some(as_of) = self.roll_stale(self.plan.stale_read_pct)
            && let Some((state, _)) = self.state_as_of(uri, as_of)
        {
            self.count_stale_read("read_text_if_exists", uri, as_of);
            return Ok(state.map(|text| self.maybe_corrupt("read_text_if_exists", uri, text)));
        }
        let out = self.inner.read_text_if_exists(uri).await?;
        Ok(out.map(|text| self.maybe_corrupt("read_text_if_exists", uri, text)))
    }
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> OmniResult<Option<String>> {
        self.read_fault("read_text_if_exists_bounded", uri).await?;
        self.latent_fault("read_text_if_exists_bounded", uri)?;
        self.note_persisted_read("read_text_if_exists_bounded", uri);
        if let Some(as_of) = self.roll_stale(self.plan.stale_read_pct)
            && let Some((state, _)) = self.state_as_of(uri, as_of)
        {
            self.count_stale_read("read_text_if_exists_bounded", uri, as_of);
            return Ok(
                state.map(|text| self.maybe_corrupt("read_text_if_exists_bounded", uri, text))
            );
        }
        let out = self
            .inner
            .read_text_if_exists_bounded(uri, max_bytes)
            .await?;
        Ok(out.map(|text| self.maybe_corrupt("read_text_if_exists_bounded", uri, text)))
    }
    async fn write_text(&self, uri: &str, contents: &str) -> OmniResult<()> {
        let _in_flight = self.kill.as_ref().map(|k| k.enter_write());
        if let WriteFate::Lost = self.write_fault("write_text", uri, true).await? {
            return Ok(());
        }
        let (target, stored);
        match self.maybe_misdirect("write_text", uri) {
            Some(t) => target = t,
            None => target = uri.to_string(),
        }
        match self.maybe_corrupt_write("write_text", &target, contents) {
            Some(s) => stored = s,
            None => stored = contents.to_string(),
        }
        self.staleness_base(&target).await;
        let out = self.inner.write_text(&target, &stored).await;
        if out.is_ok() {
            self.count_completion("write_text", uri);
            self.staleness_record(&target, Some(stored.clone()), None);
        }
        self.lose_ack("write_text", uri, out).await
    }
    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> OmniResult<bool> {
        let _in_flight = self.kill.as_ref().map(|k| k.enter_write());
        if let WriteFate::Lost = self.write_fault("write_text_if_absent", uri, true).await? {
            // The engine believes the if-absent insert landed.
            return Ok(true);
        }
        let (target, stored);
        match self.maybe_misdirect("write_text_if_absent", uri) {
            Some(t) => target = t,
            None => target = uri.to_string(),
        }
        match self.maybe_corrupt_write("write_text_if_absent", &target, contents) {
            Some(s) => stored = s,
            None => stored = contents.to_string(),
        }
        self.staleness_base(&target).await;
        let out = self.inner.write_text_if_absent(&target, &stored).await;
        if matches!(out, Ok(true)) {
            self.count_completion("write_text_if_absent", uri);
            self.staleness_record(&target, Some(stored.clone()), None);
        }
        self.lose_ack("write_text_if_absent", uri, out).await
    }
    async fn exists(&self, uri: &str) -> OmniResult<bool> {
        // DOCUMENTED EXCLUSION: no `read_fault` here — existence probes
        // are metadata, like the latent-sector carve-out on content
        // reads, so `read_error_pct`/latency never touch them and a
        // probe consuming a lost-write's absence goes uncounted.
        // Widening this to transient faults would add draws to every
        // existing fault universe (pinned seeds shift); it rides the
        // fault-vocabulary work, not a quiet edit.
        // as-of membership (stale absence or zombie presence).
        if let Some(as_of) = self.roll_stale(self.plan.stale_read_pct)
            && let Some((state, _)) = self.state_as_of(uri, as_of)
        {
            self.count_stale_read("exists", uri, as_of);
            return Ok(state.is_some());
        }
        self.inner.exists(uri).await
    }
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> OmniResult<()> {
        let _in_flight = self.kill.as_ref().map(|k| k.enter_write());
        if let WriteFate::Lost = self.write_fault("rename_text", from_uri, true).await? {
            return Ok(());
        }
        self.staleness_base(from_uri).await;
        self.staleness_base(to_uri).await;
        let moved = if self.staleness_on() {
            self.inner
                .read_text_if_exists(from_uri)
                .await
                .ok()
                .flatten()
        } else {
            None
        };
        let out = self.inner.rename_text(from_uri, to_uri).await;
        if out.is_ok() {
            self.count_completion("rename_text", from_uri);
            // write_fault's census entry carried from_uri; the store also
            // GAINS to_uri — record it too, or the bottom-listing
            // reconciliation misreads the destination as a bypass write.
            crate::write_census::record("adapter", "rename_text", to_uri, self.active());
            self.staleness_record(from_uri, None, None);
            self.staleness_record(to_uri, moved, None);
        }
        self.lose_ack("rename_text", from_uri, out).await
    }
    async fn delete(&self, uri: &str) -> OmniResult<()> {
        let _in_flight = self.kill.as_ref().map(|k| k.enter_write());
        if let WriteFate::Lost = self.write_fault("delete", uri, true).await? {
            return Ok(());
        }
        self.staleness_base(uri).await;
        let out = self.inner.delete(uri).await;
        if out.is_ok() {
            self.count_completion("delete", uri);
            self.staleness_record(uri, None, None);
        }
        self.lose_ack("delete", uri, out).await
    }
    async fn list_dir(&self, dir_uri: &str) -> OmniResult<Vec<String>> {
        self.read_fault("list_dir", dir_uri).await?;
        let mut listed = self.inner.list_dir(dir_uri).await?;
        // as-of membership (semantics on `stale_list_pct`); untracked keys
        // keep fresh membership (unknowable).
        if let Some(as_of) = self.roll_stale(self.plan.stale_list_pct) {
            // Collect candidates first, then query as-of state — state_as_of
            // takes the same lock (non-reentrant).
            let candidates: Vec<String> = {
                let map = self.key_history.lock().unwrap();
                map.keys()
                    .filter(|uri| uri.starts_with(dir_uri))
                    .cloned()
                    .collect()
            };
            for uri in candidates {
                let Some((state, _)) = self.state_as_of(&uri, as_of) else {
                    continue;
                };
                let present = state.is_some();
                let here = listed.iter().position(|l| l == &uri);
                match (present, here) {
                    (true, None) => listed.push(uri),
                    (false, Some(i)) => {
                        listed.remove(i);
                    }
                    _ => {}
                }
            }
            listed.sort();
            self.stale_lists_served
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            println!("dst s25 stale: list_dir {dir_uri} as-of={as_of}");
        }
        Ok(listed)
    }
    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: omnigraph::storage::ListDirBounds,
    ) -> OmniResult<Vec<String>> {
        self.read_fault("list_dir_bounded", dir_uri).await?;
        let mut listed = self
            .inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await?;
        if let Some(as_of) = self.roll_stale(self.plan.stale_list_pct) {
            let candidates: Vec<String> = {
                let map = self.key_history.lock().unwrap();
                map.keys()
                    .filter(|uri| uri.starts_with(dir_uri) && uri.ends_with(matching_suffix))
                    .cloned()
                    .collect()
            };
            for uri in candidates {
                let Some((state, _)) = self.state_as_of(&uri, as_of) else {
                    continue;
                };
                let present = state.is_some();
                let here = listed.iter().position(|l| l == &uri);
                match (present, here) {
                    (true, None) => listed.push(uri),
                    (false, Some(i)) => {
                        listed.remove(i);
                    }
                    _ => {}
                }
            }
            listed.sort();
            self.stale_lists_served
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            println!("dst s25 stale: list_dir_bounded {dir_uri} as-of={as_of}");
        }
        Ok(listed)
    }
    async fn read_text_versioned(&self, uri: &str) -> OmniResult<(String, String)> {
        self.read_fault("read_text_versioned", uri).await?;
        self.latent_fault("read_text_versioned", uri)?;
        self.note_persisted_read("read_text_versioned", uri);
        // the stale (content, token) PAIR — consistent with
        // each other, both old. A later CAS with the stale token fails
        // cleanly at the strict head: the "read stale, CAS saves you"
        // path, exercised. Served only when the as-of state has BOTH
        // halves (a base entry captured pre-armed, or a CAS-recorded
        // write); content-only history serves fresh.
        if let Some(as_of) = self.roll_stale(self.plan.stale_read_pct)
            && let Some((Some(text), Some(token))) = self.state_as_of(uri, as_of)
        {
            self.count_stale_read("read_text_versioned", uri, as_of);
            return Ok((self.maybe_corrupt("read_text_versioned", uri, text), token));
        }
        // Corrupt the CONTENT only, never the version token: a lying version
        // string is a CAS-metadata consistency fault (a different fault realm,
        // with the staleness family), not a byte-level content lie.
        let (text, version) = self.inner.read_text_versioned(uri).await?;
        Ok((
            self.maybe_corrupt("read_text_versioned", uri, text),
            version,
        ))
    }
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> OmniResult<Option<String>> {
        // Gauge BEFORE write_fault, like every other write method: a c=0
        // cut parks inside write_fault and asserts the gauge reads 1.
        let _in_flight = self.kill.as_ref().map(|k| k.enter_write());
        // CAS: lose/misdirect EXCLUDED (fabricating a version token is a
        // consistency lie); write-time corruption allowed
        // (content mutated, CAS semantics intact). The CAS itself is NEVER
        // stale — it executes at the strict head (the real providers'
        // contract, and the axis's judgeability anchor).
        let _ = self.write_fault("write_text_if_match", uri, false).await?;
        let stored = match self.maybe_corrupt_write("write_text_if_match", uri, contents) {
            Some(s) => s,
            None => contents.to_string(),
        };
        self.staleness_base(uri).await;
        // The CAS is ack-loss's crown jewel: lose the ack of a SUCCESSFUL
        // compare-and-swap and the caller's retry compares against a
        // version its own first attempt already advanced — the
        // self-collision scenario (does the engine misread it as a
        // conflict with another writer?).
        let out = self
            .inner
            .write_text_if_match(uri, &stored, expected_version)
            .await;
        if let Ok(Some(new_token)) = &out {
            self.count_completion("write_text_if_match", uri);
            self.staleness_record(uri, Some(stored.clone()), Some(new_token.clone()));
        }
        self.lose_ack("write_text_if_match", uri, out).await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> OmniResult<()> {
        let _in_flight = self.kill.as_ref().map(|k| k.enter_write());
        if let WriteFate::Lost = self.write_fault("delete_prefix", prefix_uri, true).await? {
            return Ok(());
        }
        let out = self.inner.delete_prefix(prefix_uri).await;
        if out.is_ok() {
            self.count_completion("delete_prefix", prefix_uri);
        }
        if out.is_ok() && self.staleness_on() {
            // Every tracked key under the prefix is now deleted at head.
            let known: Vec<String> = {
                let map = self.key_history.lock().unwrap();
                map.keys()
                    .filter(|uri| uri.starts_with(prefix_uri))
                    .cloned()
                    .collect()
            };
            for uri in known {
                self.staleness_record(&uri, None, None);
            }
        }
        self.lose_ack("delete_prefix", prefix_uri, out).await
    }
}

// ----------------------------------------------------------------- oracles --

async fn assert_matches_model(db: &Omnigraph, model: &Model, where_: &str) {
    assert_eq!(
        person_rows(db).await,
        model.person_rows(),
        "{where_}: persons diverged from model"
    );
    assert_eq!(
        knows_pairs(db).await,
        model.edge_pairs(),
        "{where_}: edges diverged from model (referential-integrity oracle)"
    );
}

/// Read the WHOLE observable store — branch list plus every branch's persons
/// and edges — in the model's deterministic order. A branch that lists but
/// cannot be read (torn create/delete) panics inside the readers: that IS the
/// oracle for torn branch state.
async fn observe_world(db: &Omnigraph) -> WorldState {
    let mut names = db.branch_list().await.expect("branch list");
    names.sort();
    let mut ordered = vec!["main".to_string()];
    ordered.extend(names.into_iter().filter(|n| n != "main"));
    let mut out = Vec::new();
    for name in ordered {
        // Boxed: the read path's future state is large and this runs nested
        // inside crash/reconcile frames (2 MiB test stack).
        let persons = Box::pin(person_rows_on(db, &name)).await;
        let edges = Box::pin(knows_pairs_on(db, &name)).await;
        out.push((name, persons, edges));
    }
    out
}

/// The recovery-obligation observation channel: sidecar residue under
/// `__recovery/`. ONE lookup shared by reconcile, the final audit, and the
/// channel-validation test (`dst_residue_channel_sees_planted_file` — the
/// permanent canary: if the engine ever moves the sidecar directory, that
/// test goes red instead of this oracle silently reading an empty
/// nonexistent path forever, the exact trap an earlier audit of this oracle walked into).
pub async fn recovery_residue(storage: &Arc<dyn StorageAdapter>, root: &str) -> Vec<String> {
    storage
        .list_dir(&format!("{root}/__recovery"))
        .await
        .expect("list recovery residue")
}

async fn assert_world_matches(db: &Omnigraph, world: &WorldModel, where_: &str) {
    assert_eq!(
        observe_world(db).await,
        world.render(),
        "{where_}: world diverged from model"
    );
}

/// record main's history as it happens: after each op (and at
/// the boundaries), read the head commit id; when it moved, snapshot the
/// model's CURRENT main state under that id. Self-synchronizing: it doesn't
/// matter which op kinds produce commits (mutations, loads, merges,
/// maintenance) — whatever advances the head gets an entry paired with the
/// model state the claim channel asserts for that moment.
/// Coherence dependency for keep-serving universes: a mid-watch capture
/// pairs the CURRENT head with a model that excludes the deferred op —
/// sound because a pending strand never advances the manifest head (a
/// partial multi-table commit does not publish), and every head-advancing
/// entry heal resolves the watch within its own iteration.
async fn capture_history(db: &Omnigraph, main: &Model, history: &mut Vec<(String, Model)>) {
    // Boxed: composes with the big engine op futures in run_universe's poll
    // frame (2 MiB test-stack trait).
    let head = Box::pin(db.resolve_snapshot("main"))
        .await
        .expect("resolve main head")
        .to_string();
    if history.last().map(|(id, _)| id != &head).unwrap_or(true) {
        history.push((head, main.clone()));
    }
}

/// the TIME-TRAVEL ORACLE: at final audit, RE-READ every
/// recorded commit through the engine's history surface and compare against
/// the model's memory of that moment. Two nets:
///
/// 1. **Snapshot equality** (unambiguous): persons via the raw scan and
///    edges via a REAL TRAVERSAL, both pinned to `ReadTarget::Snapshot`,
///    must equal the recorded model state — for EVERY commit in the
///    universe's history. A recovery or maintenance op that corrupts the
///    PAST while preserving the head passed every oracle before this one.
/// 2. **Conservative Person diff** over adjacent commits via
///    `diff_commits`: every model-changed name must appear in the engine's
///    ChangeSet, every engine-reported Delete must be a model delete, and
///    engine deletes may not exceed model deletes. Deliberately one-sided
///    where semantics are ambiguous (a value-equal upsert may legitimately
///    surface as an engine Update the model-diff can't see) — no guessing,
///    per the ghost-tie-break lesson. Edge diffs ride on net 1 (edge change
///    ids are ULIDs, not model-addressable pairs).
async fn assert_history_matches(db: &Omnigraph, history: &[(String, Model)], where_: &str) {
    for (commit_id, model) in history {
        let persons = Box::pin(person_rows_target(
            db,
            ReadTarget::snapshot(SnapshotId::new(commit_id)),
        ))
        .await;
        assert_eq!(
            persons,
            model.person_rows(),
            "{where_}: TIME-TRAVEL persons at {commit_id} diverged from the model's memory"
        );
        let edges = Box::pin(knows_pairs_target(
            db,
            ReadTarget::snapshot(SnapshotId::new(commit_id)),
        ))
        .await;
        assert_eq!(
            edges,
            model.edge_pairs(),
            "{where_}: TIME-TRAVEL edges at {commit_id} diverged from the model's memory"
        );
    }
    for pair in history.windows(2) {
        let (a_id, a_m) = &pair[0];
        let (b_id, b_m) = &pair[1];
        let filter = ChangeFilter {
            kinds: Some(vec![EntityKind::Node]),
            type_names: Some(vec!["Person".to_string()]),
            ops: None,
        };
        let cs = Box::pin(db.diff_commits(a_id, b_id, &filter))
            .await
            .expect("diff_commits over recorded history");
        let mut model_changed: BTreeSet<String> = BTreeSet::new();
        let mut model_deleted: BTreeSet<String> = BTreeSet::new();
        for (name, val) in &a_m.persons {
            match b_m.persons.get(name) {
                None => {
                    model_deleted.insert(name.clone());
                }
                Some(v) if v != val => {
                    model_changed.insert(name.clone());
                }
                _ => {}
            }
        }
        for name in b_m.persons.keys() {
            if !a_m.persons.contains_key(name) {
                model_changed.insert(name.clone());
            }
        }
        let engine_ids: BTreeSet<&str> = cs.changes.iter().map(|c| c.id.as_str()).collect();
        let engine_deletes: BTreeSet<&str> = cs
            .changes
            .iter()
            .filter(|c| matches!(c.op, ChangeOp::Delete))
            .map(|c| c.id.as_str())
            .collect();
        for name in model_changed.iter().chain(model_deleted.iter()) {
            assert!(
                engine_ids.contains(name.as_str()),
                "{where_}: diff_commits {a_id}→{b_id} is MISSING model-changed Person '{name}'"
            );
        }
        for name in &model_deleted {
            assert!(
                engine_deletes.contains(name.as_str()),
                "{where_}: diff_commits {a_id}→{b_id} did not report model-deleted Person '{name}' as Delete"
            );
        }
        for id in &engine_deletes {
            assert!(
                model_deleted.contains(*id),
                "{where_}: diff_commits {a_id}→{b_id} reports a Delete of '{id}' the model never deleted"
            );
        }
    }
}

/// the PHYSICAL-CHANNEL ORACLE (third audit channel): for every
/// branch, the stored-row dump (`export_jsonl`, NO query machinery) must equal
/// the model's PHYSICAL expectation — persons exactly, Knows = logical edges
/// ∪ ghost self-loops. The claim, query, and physical channels are three
/// independent reads of one store; any pairwise disagreement outside the
/// modeled ghost delta is a bug (claim-vs-query found #474; query-vs-physical is
/// the ghost-row detector by construction; claim-vs-physical catches silent lost
/// writes on paths the query channel never touches).
async fn assert_physical_matches(db: &Omnigraph, world: &WorldModel, where_: &str) {
    for branch in world.branch_names() {
        let (persons, knows) = Box::pin(physical_view_on(db, &branch)).await;
        let m = world.state_of(&branch);
        assert_eq!(
            persons,
            m.person_rows(),
            "{where_}: EXPORT persons diverged from model on '{branch}'"
        );
        let expected = m.edges_with_ghosts();
        assert_eq!(
            knows, expected,
            "{where_}: EXPORT Knows diverged from model (logical ∪ ghosts) on '{branch}'"
        );
    }
}

/// INPUT CONTRACT — the catalog reads `world` to judge the refusal; a
/// post-ruling world is the correct input (the engine state that produced
/// the refusal included the healed strand). Every member must be an
/// ENTRY-TIME refusal with no durable effects: the no-interrupt resolution
/// path relies on that property, and a future member with durable effects
/// would silently break it. The dead-target member below covers ops whose
/// model target a ruling removed.
fn is_legal_rejection(
    err: &OmniError,
    world: &WorldModel,
    wop: &WorldOp,
    expected_conflict: bool,
) -> bool {
    // Dead-target member: an op whose target is absent from the (post-
    // ruling) world fails legally per se — the engine refuses a missing
    // branch with its not-found/already-exists spellings, entry-time and
    // effect-free. Without this member a mid-watch heal that changes
    // branch topology turns a correct refusal into a false LegalClaim red.
    if !op_targets_live(world, wop) {
        return true;
    }
    let text = format!("{err:?}");
    if text.contains(FAULT_MARKER) {
        return true;
    }
    // a lost acknowledgement is a legal failure — the op's
    // effects may well be durable; reconcile arbitrates which picture holds.
    if text.contains(ACK_LOSS_MARKER) {
        return true;
    }
    // a latent sector error is an injected (honest, marked)
    // fault — legal like FAULT_MARKER. Engine-born detection errors from
    // CORRUPTED reads carry no marker by construction; those are legalized
    // by ledger-overlap attribution at the call site, not here.
    if text.contains(LATENT_MARKER) {
        return true;
    }
    if expected_conflict && is_merge_conflict_err(err) {
        return true;
    }
    if is_recovery_barrier_rejection(wop, err) {
        return true;
    }
    // RI hypothesis: deleting a person with live edges may be refused.
    // `state_of_opt`: the branch can be absent from a post-ruling world.
    if let WorldOp::Data {
        branch,
        op: Op::DeletePerson { name },
    } = wop
        && world
            .state_of_opt(branch)
            .is_some_and(|m| m.has_edges_touching(name))
        && (text.contains("referential") || text.contains("edge"))
    {
        return true;
    }
    false
}

/// How a failed op's world state settled after reconcile + recovery reopen.
/// Doubles as an op's standing inside a composition hypothesis
/// ([`composition_hypotheses`]). Declaration order IS the arbitration's
/// preference order (`Ord`): more-applied wins ties.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum ReconcileOutcome {
    /// The op left no trace (rolled back / never landed).
    NotApplied,
    /// The fork-survives half-state: a crashed `LoadFork`'s implicit fork
    /// survived while the load itself rolled back — a durable base-state
    /// branch with none of the payload. Known, documented behavior (Azim's
    /// pending fork-survives contract call), accepted for exactly this op kind.
    ForkOnly,
    /// The op survived recovery (rolled forward / was already durable).
    Applied,
}

impl ReconcileOutcome {
    /// Fold this outcome's effect into the model — the ONE spelling of the
    /// outcome→apply match.
    fn apply(self, world: &mut WorldModel, wop: &WorldOp) {
        match self {
            ReconcileOutcome::NotApplied => {}
            ReconcileOutcome::ForkOnly => apply_fork_only(world, wop),
            ReconcileOutcome::Applied => apply_world(world, wop),
        }
    }
}

#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
/// MAINTENANCE-OBLIGATION ORACLES. For empty-model-delta
/// maintenance ops (Optimize/Cleanup/EnsureIndices) the two-sided crash
/// contract's hypotheses COINCIDE — their reconcile verdicts are ties
/// (measured: Optimize alone owns 44% of all crash states), so a maintenance
/// death was judged only on bricks and residue. These obligations give
/// those deaths content. Runs AFTER reconcile (recovery reopen done, world
/// model arbitrated), fault-suspended by the caller — the engine is judged,
/// not the injected weather. Returns true iff the dead op was maintenance
/// and the obligation pass ran (feeds `UniverseReport.maintenance_reruns`).
///
/// (1) IDEMPOTENT CONVERGENCE — rerun the SAME op; it must succeed. A
///     crashed maintenance op must never wedge its own retry (the
///     reopen-heals / cleanup-brick failure shape, this time asserted).
/// (2) HEAD READABLE AFTER CLEANUP — post-crashed-Cleanup, the whole
///     observable world must read back equal to the model via real
///     traversal: a partial GC that dropped a manifest-referenced version
///     dies here instead of hiding behind the tie-verdict.
/// (3) INDEX-DATA CONSISTENCY — post-crashed-Optimize/EnsureIndices, the
///     three-arm traversal differential must hold IMMEDIATELY (the
///     oracle, whose seeded-CSR-bug red proof carries over) — a corrupted
///     index can't wait for the next scheduled check where a heal might
///     mask it.
///
/// Sensitivity: `Scenario.fail_maintenance_rerun` arms a REAL
/// engine failpoint around the rerun so the convergence assert provably
/// fires — `dst_sensitivity_maintenance_rerun_failure_is_red`.
async fn maintenance_obligations(
    db: &mut Omnigraph,
    world: &WorldModel,
    wop: &WorldOp,
    label: &str,
    at_op: usize,
    fail_rerun: bool,
) -> bool {
    let WorldOp::Data { op, .. } = wop else {
        return false;
    };
    let (rerun_window, kind) = match op {
        Op::Optimize => ("optimize.before_compact", "Optimize"),
        Op::Cleanup => ("cleanup.post_recovery_check_pre_gates", "Cleanup"),
        Op::EnsureIndices => (
            "ensure_indices.post_phase_b_pre_manifest_commit",
            "EnsureIndices",
        ),
        _ => return false,
    };
    // (1) Idempotent convergence.
    let rerun = {
        #[cfg(feature = "failpoints")]
        let _sensitivity =
            fail_rerun.then(|| omnigraph::failpoints::ScopedFailPoint::new(rerun_window, "return"));
        #[cfg(not(feature = "failpoints"))]
        let _ = (rerun_window, fail_rerun);
        exec_world_op(db, wop).await
    };
    if let Err(error) = rerun {
        detectors::violation(
            DET_MAINTENANCE,
            at_op,
            format!(
                "{label}: MAINTENANCE OBLIGATION violated — {kind} rerun after recovery \
                 must converge (idempotent convergence), got: {error:?}"
            ),
            "a crashed maintenance op's rerun converges",
        );
    }
    match op {
        Op::Cleanup => {
            // (2) Whole observable world back via real traversal.
            let visible = observe_world(db).await;
            if visible != world.render() {
                detectors::violation(
                    DET_MAINTENANCE,
                    at_op,
                    format!(
                        "{label}: MAINTENANCE OBLIGATION violated — post-crashed-Cleanup read \
                         diverged (retention must not drop manifest-referenced state); \
                         visible={visible:?}"
                    ),
                    "the whole observable world stays readable after a crashed Cleanup",
                );
            }
        }
        Op::Optimize | Op::EnsureIndices => {
            // (3) Index-data consistency, immediately — the traversal funnel
            // reused under the MAINTENANCE tag (a red here is the obligation
            // firing, not the standalone differential).
            let mut branches = vec!["main".to_string()];
            branches.extend(world.branches.keys().cloned());
            detectors::tagged(
                DET_MAINTENANCE,
                at_op,
                Box::pin(assert_traversal_modes_agree(db, world, &branches, label)),
            )
            .await;
        }
        _ => unreachable!("gated above"),
    }
    true
}

/// Reopen over the same storage after a failure — the recovery sweep. The
/// caller drops the failed handle first. Carries the DOUBLE-FAULT lever
/// (kill the FIRST recovery sweep mid-pass, then prove a second clean
/// reopen still converges) and the bounded retry: the recovery sweep can
/// ITSELF hit injected faults (it writes sidecars) — a real client
/// retries. Bounded and seeded, so still deterministic. Shared by
/// [`reconcile_after_failure`] and [`reconcile_watch_resolution`].
async fn reopen_under_storm(
    storage: &Arc<dyn StorageAdapter>,
    root: &str,
    label: &str,
    at_op: usize,
    recovery_crash: Option<&'static str>,
) -> Omnigraph {
    #[cfg(feature = "failpoints")]
    if let Some(rc) = recovery_crash {
        let _fp = omnigraph::failpoints::ScopedFailPoint::new(rc, "return");
        // Best-effort double fault: if the window IS on this crash's recovery
        // path, the first recovery sweep dies here and we prove a SECOND clean reopen
        // still converges (below). If it isn't reached, no double fault
        // happened — window reachability is workload-dependent (the hunt's
        // lesson); fall through to the normal reopen either way.
        let _ = Box::pin(Omnigraph::open_with_storage(root, storage.clone())).await;
        // guard drops here → recovery crash no longer scheduled for the real reopen below
    }
    #[cfg(not(feature = "failpoints"))]
    let _ = recovery_crash;

    const REOPEN_ATTEMPT_CAP: u32 = 16;
    let mut reopen_attempts = 0u32;
    loop {
        match Box::pin(Omnigraph::open_with_storage(root, storage.clone())).await {
            Ok(db) => break db,
            Err(e) => {
                let text = format!("{e:?}");
                if !text.contains(FAULT_MARKER) {
                    detectors::violation(
                        DET_CRASH_CONTRACT,
                        at_op,
                        format!("{label}: reopen failed for a NON-injected reason: {e:?}"),
                        "the recovery reopen fails only on injected faults",
                    );
                }
                reopen_attempts += 1;
                assert!(
                    reopen_attempts < REOPEN_ATTEMPT_CAP,
                    "{label}: recovery never survived the fault storm \
                     ({REOPEN_ATTEMPT_CAP} attempts; last error: {e:?})"
                );
            }
        }
    }
}

/// RECOVERY-OBLIGATION ORACLE (2026-08-12, from the seeded-recovery-no-op
/// honesty experiment): the state hypotheses alone CANNOT distinguish a
/// correct rollback from a recovery that silently did nothing — "not
/// applied" is always legal, and the barrier carve-out excuses subsequent
/// rejections. With heal_pending_sidecars_roll_forward stubbed to a
/// no-op, 25 of 26 tests stayed green; only the discovery-5 pin (which
/// asserts the healing SIDE-EFFECT) went red. So assert recovery's
/// obligation, not just state legality: a successful read-write reopen
/// leaves no sidecar residue (the reopen-heals contract).
/// Runs fault-suspended (callers suspend around reconcile), so this read
/// is clean. Shared by [`reconcile_after_failure`] and
/// [`reconcile_watch_resolution`].
async fn assert_no_recovery_residue(
    storage: &Arc<dyn StorageAdapter>,
    root: &str,
    label: &str,
    at_op: usize,
) {
    let residue = recovery_residue(storage, root).await;
    // Persisted tier: foreign-named (injected-misdirect) residue is the named
    // carve-out `s11b-foreign-sidecar-ignored` — recorded, tolerated;
    // real-named residue still panics (reopen heals what it recognizes).
    let residue = partition_residue(residue, root, label);
    if !residue.is_empty() {
        detectors::violation(
            DET_RECOVERY_OBLIGATION,
            at_op,
            format!(
                "{label}: recovery reopen left sidecar residue (recovery dead or incomplete?): {residue:?}"
            ),
            "a successful read-write reopen leaves __recovery/ empty",
        );
    }
}

/// After ANY failed op (crash window or injected fault): assert atomicity,
/// reopen over the same storage (= the recovery sweep; a
/// fault-killed mutation arms a recovery sidecar and the engine BLOCKS
/// further writes behind a recovery barrier until a read-write reopen), then
/// assert recovery monotonicity and report how the op settled. World-level:
/// the hypotheses (op invisible
/// XOR op applied, plus the fork-survives third state for `LoadFork`) cover
/// branch existence and every branch's state, so torn branch
/// creates/deletes/merges violate atomicity exactly like torn mutations.
///
/// INPUT CONTRACT — at most ONE unjudged op: `world` must hold the settled
/// truth of every op except `wop`, whose fate is the single open question
/// the hypotheses cover. A keep-serving resolution violates that contract
/// (two ops unjudged: the deferred op and the interrupting op) and uses
/// [`reconcile_watch_resolution`] instead.
#[allow(clippy::too_many_arguments)]
async fn reconcile_after_failure(
    db: Omnigraph,
    storage: Arc<dyn StorageAdapter>,
    root: &str,
    wop: &WorldOp,
    world: &WorldModel,
    label: &str,
    at_op: usize,
    recovery_crash: Option<&'static str>,
) -> (Omnigraph, ReconcileOutcome, &'static str) {
    // Stale-capture rule on [`resolve_keep_serving_watch`]: a ruling can
    // remove the op's target between its sampling and this judgment. A
    // dead-target op has exactly one legal outcome — NotApplied — enforced
    // below at the outcome derivation and the tie-break gate, not just the
    // `as_with` build.
    let target_live = op_targets_live(world, wop);
    let with = {
        let mut w = world.clone();
        if target_live {
            apply_world(&mut w, wop);
        }
        w
    };
    let visible = observe_world(&db).await;
    let as_model = world.render();
    let as_with = with.render();
    let as_fork_only = match wop {
        WorldOp::LoadFork { .. } => {
            let mut w = world.clone();
            apply_fork_only(&mut w, wop);
            Some(w.render())
        }
        _ => None,
    };
    let legal = |state: &WorldState| {
        *state == as_model || *state == as_with || as_fork_only.as_ref() == Some(state)
    };
    if !legal(&visible) {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!("{label}: PARTIAL application (op={wop:?}); visible={visible:?}"),
            "atomicity: the post-failure world renders as base, applied, or fork-only",
        );
    }
    let committed = visible == as_with;
    let fork_was_visible = as_fork_only.as_ref() == Some(&visible);

    drop(db);
    let db = reopen_under_storm(&storage, root, label, at_op, recovery_crash).await;
    let after = observe_world(&db).await;
    if !legal(&after) {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!("{label}: recovery produced an illegal state (op={wop:?}); after={after:?}"),
            "post-recovery world renders as base, applied, or fork-only",
        );
    }
    assert_no_recovery_residue(&storage, root, label, at_op).await;
    if committed && after != as_with {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!("{label}: recovery DEMOTED a committed write; after={after:?}"),
            "recovery monotonicity: a committed write stays applied",
        );
    }
    if fork_was_visible && after == as_model {
        // Fork-survives oracle: the implicit fork is a fully published branch create;
        // recovery must never delete it (it may still roll the LOAD forward).
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!("{label}: recovery DELETED a durably created implicit fork branch"),
            "recovery monotonicity: a durably created fork branch survives recovery",
        );
    }
    let mut outcome = if target_live && after == as_with {
        ReconcileOutcome::Applied
    } else if as_fork_only.as_ref() == Some(&after) {
        ReconcileOutcome::ForkOnly
    } else {
        // Includes the dead-target case: `as_with == as_model` there, and
        // the Applied arm above is gated off so the collapse cannot be
        // mislabeled Applied (and the caller's `outcome.apply` stays a
        // no-op instead of panicking or overwriting a slot).
        ReconcileOutcome::NotApplied
    };
    // Which channel the ruling rests on — recorded so the run tables carry
    // observed provenance, never an assumption (canary lesson).
    let mut channel: &'static str = "query";
    // GHOST TIE-BREAK (the physical-channel oracle's first catch,
    // found in its first full-suite run, 2026-08-11): an op whose ONLY effect
    // is on ghost rows (a failed self-loop add_friend; a remove-from touching
    // nothing but ghosts) is invisible to every query-channel read — the
    // two hypotheses render identically, and the judgment above silently
    // guesses. Before the physical-channel oracle existed, that guess quietly
    // recorded ghosts that never landed (caught at final audit). The raw
    // channel is the one read that can resolve it: consult it for the
    // touched branch. `target_live` gate: a dead-target collapse also makes
    // the renders equal, but its branch is gone — nothing to consult.
    if target_live && as_model == as_with {
        let touched = match wop {
            WorldOp::Data { branch, .. } => Some(branch),
            _ => None,
        };
        if let Some(branch) = touched {
            let g_world = &world.state_of(branch).ghosts;
            let g_with = &with.state_of(branch).ghosts;
            if g_world != g_with {
                channel = "query+physical";
                let (_, knows) = Box::pin(physical_view_on(&db, branch)).await;
                let expect_with = with.state_of(branch).edges_with_ghosts();
                let expect_world = world.state_of(branch).edges_with_ghosts();
                outcome = if knows == expect_with {
                    ReconcileOutcome::Applied
                } else if knows == expect_world {
                    ReconcileOutcome::NotApplied
                } else {
                    detectors::violation(
                        DET_ARBITRATION_PHYSICAL,
                        at_op,
                        format!(
                            "{label}: physical channel matches NEITHER ghost hypothesis \
                             (op={wop:?}, physical={knows:?})"
                        ),
                        "the export tie-break resolves ghost-only effects to one hypothesis",
                    )
                };
            }
        }
    }
    (db, outcome, channel)
}

// ------------------------------------------- traversal modes ----

/// TRAVERSAL-MODE DIFFERENTIAL: the engine's two complete
/// Expand implementations (per-hop INDEXED scans vs the in-memory CSR walk)
/// must agree with each other AND the model in every state a universe can
/// produce — post-crash, post-merge, post-optimize, fault survivors — not
/// just proptest's clean stores. The model arm covers the common-mode blind
/// spot (#474: both gated modes shared the visited gate and AGREED on the
/// wrong answer). The third arm is the BOUND-EDGE spelling, which scans
/// rows without the gate: its disagreement with the gated modes must equal
/// the ghost set EXACTLY — #474's known inconsistency as a modeled,
/// continuously-checked delta (flips to plain equality when the fix lands).
/// Observer note: a forced-CSR read may build/cache the graph index — a
/// deterministic cache-warming side effect auto mode could equally cause;
/// accepted and recorded.
async fn assert_traversal_modes_agree(
    db: &Omnigraph,
    world: &WorldModel,
    branches: &[String],
    where_: &str,
) {
    for branch in branches {
        let expected = world.state_of(branch).edge_pairs();
        let indexed = Box::pin(knows_pairs_target_mode(
            db,
            ReadTarget::branch(branch),
            "indexed",
        ))
        .await;
        let csr = Box::pin(knows_pairs_target_mode(
            db,
            ReadTarget::branch(branch),
            "csr",
        ))
        .await;
        if indexed != csr || indexed != expected {
            panic!(
                "{where_}: TRAVERSAL-MODE divergence on '{branch}' — indexed {} model, \
                 csr {} model, indexed {} csr. indexed={indexed:?} csr={csr:?} model={expected:?}",
                if indexed == expected { "==" } else { "!=" },
                if csr == expected { "==" } else { "!=" },
                if indexed == csr { "==" } else { "!=" },
            );
        }
        // Bound arm: gated ∪ ghosts, exactly.
        let bound = Box::pin(knows_pairs_bound_target(db, ReadTarget::branch(branch))).await;
        let expected_bound = world.state_of(branch).edges_with_ghosts();
        assert_eq!(
            bound, expected_bound,
            "{where_}: BOUND-ARM divergence on '{branch}' — the bound-edge spelling must \
             equal gated ∪ ghosts (zero since the #474 fix — a nonempty delta is a regression)"
        );
    }
}

// --------------------------------------------- handle staleness --

/// Named anomaly classes for the session oracle (Elle-style: the failure
/// carries its own triage).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionAnomaly {
    /// The bystander's view matches NO recorded (or current) state — a view
    /// no version of the store ever had. Always a bug.
    FabricatedState,
    /// The bystander's view matches only states OLDER than one it already
    /// served — the session-guarantee "monotonic reads" violated.
    NonMonotonicRead,
}

/// One session's observed main view: (person rows, Knows pairs).
pub type PersonRow = (String, i64, i64);
pub type EdgePair = (String, String);
pub type SessionState = (Vec<PersonRow>, Vec<EdgePair>);

/// Pure classifier for the bystander contract: a never-writing session may
/// lawfully be BEHIND, never incoherent. Its view must equal SOME state in
/// `states` (searched newest-first, so duplicate states resolve to the
/// newest index), at an index not older than the last it served. Pure so
/// the honesty proofs can unit-test it without an engine.
pub fn classify_bystander_view(
    view: (&[PersonRow], &[EdgePair]),
    states: &[SessionState],
    last_index: Option<usize>,
) -> Result<usize, SessionAnomaly> {
    let matched = states
        .iter()
        .enumerate()
        .rev()
        .find(|(_, (p, e))| p.as_slice() == view.0 && e.as_slice() == view.1)
        .map(|(idx, _)| idx);
    match matched {
        None => Err(SessionAnomaly::FabricatedState),
        Some(idx) => match last_index {
            Some(last) if idx < last => Err(SessionAnomaly::NonMonotonicRead),
            _ => Ok(idx),
        },
    }
}

/// the SESSION ORACLE: three sessions, three contracts.
///
/// - ACTOR (performed every op): strict equality with FRESH — it just did
///   the op, no excuse to differ. On mismatch the MODEL arbitrates (fresh
///   reads through the open path and is NOT read-path-independent; a
///   `fresh != model` verdict indicts the open path itself).
/// - BYSTANDER (born at universe start, never writes — the server's
///   warm-idle shape): COHERENCE — view ∈ recorded history ∪ {current},
///   monotone. The current world state is a legal member because the
///   history capture at loop top records the PREVIOUS op's head; a
///   read-time-fresh bystander lawfully sees the not-yet-captured present.
/// - FRESH (read-only open per check, dropped): the stateless control arm.
///
/// `do_catch_up` runs the SCHEDULED cure check: `sync_branch("main")` on
/// the bystander (side-effect-free resync — deliberately NOT `refresh()`,
/// whose roll-forward healer would perturb the world the oracle judges),
/// after which the bystander must equal fresh (`StaleAfterSync`).
#[allow(clippy::too_many_arguments)]
async fn check_sessions(
    actor: &Omnigraph,
    bystander: &Omnigraph,
    root: &str,
    storage: &Arc<dyn StorageAdapter>,
    world: &WorldModel,
    history: &[(String, Model)],
    bystander_last: &mut Option<usize>,
    bystander_trail: &mut Vec<usize>,
    do_catch_up: bool,
    where_: &str,
) {
    let fresh = Box::pin(Omnigraph::open_read_only_with_storage(
        root,
        storage.clone(),
    ))
    .await
    .expect("open fresh session");

    // Schema fingerprint — the dimension row equality cannot see
    // (the schema-add finding lives here).
    assert_eq!(
        *actor.schema_source(),
        *fresh.schema_source(),
        "{where_}: ActorDrift(schema) — actor's schema differs from a fresh session's"
    );

    let a_p = Box::pin(person_rows(actor)).await;
    let a_e = Box::pin(knows_pairs(actor)).await;
    let f_p = Box::pin(person_rows(&fresh)).await;
    let f_e = Box::pin(knows_pairs(&fresh)).await;
    if a_p != f_p || a_e != f_e {
        let m_p = world.main.person_rows();
        let m_e = world.main.edge_pairs();
        let actor_matches_model = a_p == m_p && a_e == m_e;
        let fresh_matches_model = f_p == m_p && f_e == m_e;
        panic!(
            "{where_}: ActorDrift — actor and fresh sessions disagree; arbiter: \
             actor {} model, fresh {} model (fresh≠model indicts the OPEN PATH). \
             actor=({a_p:?}, {a_e:?}) fresh=({f_p:?}, {f_e:?})",
            if actor_matches_model { "==" } else { "!=" },
            if fresh_matches_model { "==" } else { "!=" },
        );
    }

    let b_p = Box::pin(person_rows(bystander)).await;
    let b_e = Box::pin(knows_pairs(bystander)).await;
    let mut states: Vec<SessionState> = history
        .iter()
        .map(|(_, m)| (m.person_rows(), m.edge_pairs()))
        .collect();
    // The not-yet-captured present is a legal (and the newest) member.
    states.push((world.main.person_rows(), world.main.edge_pairs()));
    match classify_bystander_view((&b_p, &b_e), &states, *bystander_last) {
        Ok(idx) => {
            *bystander_last = Some(idx);
            bystander_trail.push(idx);
        }
        Err(anomaly) => {
            let b_matches_model = b_p == world.main.person_rows() && b_e == world.main.edge_pairs();
            panic!(
                "{where_}: {anomaly:?} — bystander view (last served index {:?}, \
                 {} known states, bystander {} current model) view=({b_p:?}, {b_e:?})",
                *bystander_last,
                states.len(),
                if b_matches_model { "==" } else { "!=" },
            );
        }
    }

    if do_catch_up {
        Box::pin(bystander.sync_branch("main"))
            .await
            .expect("bystander scheduled sync_branch");
        let b_p = Box::pin(person_rows(bystander)).await;
        let b_e = Box::pin(knows_pairs(bystander)).await;
        assert!(
            b_p == f_p && b_e == f_e,
            "{where_}: StaleAfterSync — bystander still differs from fresh AFTER \
             sync_branch (the invalidation API failed to bring the session current). \
             bystander=({b_p:?}, {b_e:?}) fresh=({f_p:?}, {f_e:?})"
        );
        // Synced ⇒ at the newest known state.
        *bystander_last = Some(states.len().saturating_sub(1));
    }
}

// ------------------------------------------------------------ crash window --

/// What happened when an op ran under a scheduled crash window.
enum CrashOutcome {
    /// The window is not on this op's path — the op simply succeeded.
    OpSucceeded,
    /// The op failed for its OWN legal reason (model-predicted merge
    /// conflict), not the scheduled window — no crash to reconcile.
    LegalRejection,
    /// The window was hit; reconcile ran; `outcome` = how the op settled,
    /// `channel` = the observation channel(s) the arbitration rested on
    /// (values documented on `UniverseReport::reconcile_verdicts`).
    Crashed {
        outcome: ReconcileOutcome,
        channel: &'static str,
    },
}

#[cfg(feature = "failpoints")]
#[allow(clippy::too_many_arguments)]
async fn crash_op(
    db: Omnigraph,
    storage: Arc<dyn StorageAdapter>,
    root: &str,
    wop: &WorldOp,
    failpoint: &'static str,
    world: &WorldModel,
    at_op: usize,
    recovery_crash: Option<&'static str>,
    expected_conflict: bool,
    failing: Option<&FailingStorage>,
) -> (Omnigraph, CrashOutcome) {
    let mut db = db;
    let result = {
        let _fp = omnigraph::failpoints::ScopedFailPoint::new(failpoint, "return");
        exec_world_op(&mut db, wop).await
    };
    match result {
        Ok(()) => {
            if expected_conflict {
                detectors::violation(
                    DET_MERGE_PREDICTION,
                    at_op,
                    format!("engine ACCEPTED a merge the model predicts conflicts (op={wop:?})"),
                    "the engine's accept/conflict decision matches the three-way prediction",
                );
            }
            return (db, CrashOutcome::OpSucceeded);
        }
        Err(err) if expected_conflict && is_merge_conflict_err(&err) => {
            return (db, CrashOutcome::LegalRejection);
        }
        Err(_) => {}
    }
    // Reconcile runs FAULT-SUSPENDED like the kill and injected-fault
    // reconciles: its reopen-retry loop legalizes only FAULT_MARKER, so
    // composed weather (ack-loss, latent reads) landing inside the
    // reconcile would otherwise mint false crash-contract verdicts.
    if let Some(f) = failing {
        f.suspend();
    }
    let (db, outcome, channel) = Box::pin(reconcile_after_failure(
        db,
        storage,
        root,
        wop,
        world,
        &format!("crash window '{failpoint}'"),
        at_op,
        recovery_crash,
    ))
    .await;
    if let Some(f) = failing {
        f.resume();
    }
    (db, CrashOutcome::Crashed { outcome, channel })
}

#[cfg(not(feature = "failpoints"))]
#[allow(clippy::too_many_arguments)]
async fn crash_op(
    _db: Omnigraph,
    _storage: Arc<dyn StorageAdapter>,
    _root: &str,
    _wop: &WorldOp,
    _failpoint: &'static str,
    _world: &WorldModel,
    _at_op: usize,
    _recovery_crash: Option<&'static str>,
    _expected_conflict: bool,
    _failing: Option<&FailingStorage>,
) -> (Omnigraph, CrashOutcome) {
    panic!("crash scenarios require --features failpoints");
}

// --------------------------------------------------- birth universes (05) --

/// Classification of one die-during-birth universe against the BIRTH
/// CONTRACT: a store whose init died mid-sequence must open
/// cleanly, fail with a truthful diagnosis, or be cleanly re-initializable on
/// the same root — never a misdiagnosed permanent brick. Today's engine has
/// ONE known brick (#483's torn-init misdiagnosis); it is pinned as its
/// own variant so `dst_birth_contract_sweep` keeps hunting for NEW classes without
/// re-flagging the already-filed #483 brick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BirthOutcome {
    /// Init reported success despite the scheduled window (not on the
    /// executed path, or absorbed) and the store is usable.
    InitSurvived,
    /// Init died; a plain reopen succeeds and the store is usable — the
    /// pure lost-acknowledgement shape (all durable steps completed).
    DiedThenOpensClean,
    /// Init died; reopen refuses; a plain re-init on the SAME root succeeds
    /// and the store is then usable — clean recovery.
    DiedThenReinitRecovers,
    /// Init died; reopen refuses with #483's KNOWN misdiagnosis ("created by
    /// omnigraph 0.3.1 or earlier") and re-init fails in its known phase-4
    /// shapes — the #483 brick, carved out like the version collision.
    KnownTornInitBrick,
    /// Init died; reopen AND re-init both refuse in a shape that is NOT the
    /// filed #483 brick — a new birth-contract violation class.
    /// `dst_birth_contract_sweep` treats any occurrence as a finding.
    /// Messages are root-normalized.
    Stuck { reopen: String, reinit: String },
}

#[cfg(feature = "failpoints")]
fn normalize_birth_msg(msg: &str, root: &str) -> String {
    let mut out = msg.replace(root, "<root>");
    out.truncate(160);
    out
}

/// Run ONE die-during-birth universe: schedule a crash at `window`, attempt
/// init on a fresh
/// in-memory world, then judge the resulting store against the birth
/// contract. Deterministic (fixed-seed runtime, no workload randomness);
/// `dst_birth_contract_sweep` runs each window twice and asserts identical outcomes.
#[cfg(feature = "failpoints")]
pub fn run_birth_universe(root: &'static str, window: &'static str) -> BirthOutcome {
    // DELIBERATELY NOT INSTALLED: seeded ULID/clock seams, entropy
    // arming, and the 16 MiB universe thread — birth universes run a
    // fixed handful of init calls with no workload randomness and no
    // deep engine futures; per-call Box::pin covers the stack.
    detectors::install_violation_panic_hook();
    clear_process_slots();
    crate::env_knobs::require_pool_env();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&1u64.to_le_bytes()))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(async move {
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());

        let init_result = {
            let _fp = omnigraph::failpoints::ScopedFailPoint::new(window, "return");
            Box::pin(Omnigraph::init_with_storage(
                root,
                TEST_SCHEMA,
                storage.clone(),
                InitOptions::default(),
            ))
            .await
        };

        async fn usable(db: &Omnigraph) -> bool {
            load_jsonl(db, TEST_DATA, LoadMode::Overwrite).await.is_ok()
                && !person_rows(db).await.is_empty()
        }

        // Birth-contract reds route through DET_BIRTH like the sibling
        // open-crash universe, so fleet failure rows carry the detector.
        match init_result {
            Ok(db) => {
                if !usable(&db).await {
                    detectors::violation(
                        DET_BIRTH,
                        0,
                        format!("init survived {window} but store unusable"),
                        "an init that reports success yields a usable store",
                    );
                }
                BirthOutcome::InitSurvived
            }
            Err(_) => {
                match Box::pin(Omnigraph::open_with_storage(root, storage.clone())).await {
                    Ok(db) => {
                        if !usable(&db).await {
                            detectors::violation(
                                DET_BIRTH,
                                0,
                                format!("reopen after {window} death succeeded but store unusable"),
                                "a successful reopen yields a usable store",
                            );
                        }
                        BirthOutcome::DiedThenOpensClean
                    }
                    Err(open_err) => {
                        let reopen = format!("{open_err:?}");
                        // RO must agree with RW — a split-brain birth state
                        // (RW refuses, RO serves) would be its own finding.
                        if Box::pin(Omnigraph::open_read_only_with_storage(
                            root,
                            storage.clone(),
                        ))
                        .await
                        .is_ok()
                        {
                            detectors::violation(
                                DET_BIRTH,
                                0,
                                format!(
                                    "{window}: RW open refused but RO open SUCCEEDED \
                                     (split-brain birth state)"
                                ),
                                "read-only and read-write opens agree on a dead birth",
                            );
                        }
                        let reinit_result = Box::pin(Omnigraph::init_with_storage(
                            root,
                            TEST_SCHEMA,
                            storage.clone(),
                            InitOptions::default(),
                        ))
                        .await;
                        match reinit_result {
                            Ok(db) => {
                                if !usable(&db).await {
                                    detectors::violation(
                                        DET_BIRTH,
                                        0,
                                        format!(
                                            "re-init after {window} death succeeded but \
                                             store unusable"
                                        ),
                                        "a successful re-init yields a usable store",
                                    );
                                }
                                BirthOutcome::DiedThenReinitRecovers
                            }
                            Err(reinit_err) => {
                                let reinit = format!("{reinit_err:?}");
                                // #483's filed brick: the ancient-version
                                // misdiagnosis + its known re-init refusals.
                                if reopen.contains("0.3.1 or earlier")
                                    && (reinit.contains("already exists")
                                        || reinit.contains("AlreadyInitialized"))
                                {
                                    BirthOutcome::KnownTornInitBrick
                                } else {
                                    BirthOutcome::Stuck {
                                        reopen: normalize_birth_msg(&reopen, root),
                                        reinit: normalize_birth_msg(&reinit, root),
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

/// Death-during-OPEN sibling: init + load CLEAN, then schedule a crash at
/// `window` over a reopen. Contract: an injected open failure must be
/// EFFECT-FREE — the next (no-crash) open must succeed with the data intact.
/// Returns whether the crash-scheduled open actually failed (false = window
/// not on the open path).
#[cfg(feature = "failpoints")]
pub fn run_open_crash_universe(root: &'static str, window: &'static str) -> bool {
    // Same deliberate omissions as `run_birth_universe` (its note).
    detectors::install_violation_panic_hook();
    clear_process_slots();
    crate::env_knobs::require_pool_env();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(&1u64.to_le_bytes()))
        .build_local(Default::default())
        .expect("seeded runtime");
    runtime.block_on(async move {
        let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
        let db = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("clean init");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("clean load");
        let baseline = person_rows(&db).await;
        drop(db);

        let crashing_open = {
            let _fp = omnigraph::failpoints::ScopedFailPoint::new(window, "return");
            Box::pin(Omnigraph::open_with_storage(root, storage.clone())).await
        };
        let died = crashing_open.is_err();
        drop(crashing_open);

        let db = Box::pin(Omnigraph::open_with_storage(root, storage.clone()))
            .await
            .unwrap_or_else(|e| {
                detectors::violation(
                    DET_BIRTH,
                    0,
                    format!("{window}: open after an injected open failure must succeed: {e:?}"),
                    "a crashed open is effect-free (the next open succeeds)",
                )
            });
        let rows = person_rows(&db).await;
        if rows != baseline {
            detectors::violation(
                DET_BIRTH,
                0,
                format!("{window}: injected open failure DAMAGED the store; rows={rows:?}"),
                "a crashed open is effect-free (data unchanged)",
            );
        }
        died
    })
}

// ---------------------------------------------------------------- universe --

/// Consumed across the crate boundary — the pinned panel's shape assert
/// keys on it, so the spelling lives in exactly one place.
pub const KEEP_SERVING_DEFER_PREFIX: &str = "keep-serving-defer@";
/// Watch-resolution row prefixes — same one-spelling rule as the defer
/// prefix: the widened regression test's resolution-row assert keys on
/// these, so producer and reader share the consts.
pub const KEEP_SERVING_HEALED_PREFIX: &str = "keep-serving-healed@";
pub const KEEP_SERVING_INTERRUPTED_PREFIX: &str = "keep-serving-interrupted@";
pub const KEEP_SERVING_EXPIRED_PREFIX: &str = "keep-serving-expired@end";

/// One spelling of the defer row (`keep-serving-defer@op<i>:<tail>` where
/// the tail is the refused operation id, or `recovery-barrier` for the
/// clean-recovery-state spelling that names none).
fn keep_serving_defer_row(i: usize, tail: &str) -> String {
    format!("{KEEP_SERVING_DEFER_PREFIX}op{i}:{tail}")
}

/// KEEP-SERVING watch (issue #554): the pending recovery operation the live
/// handle is currently refused on, with reconcile's REOPEN withheld — never
/// the judgment: the deferred op's two-picture arbitration runs at watch
/// resolution ([`resolve_keep_serving_watch`]).
struct KeepServingWatch {
    operation_id: String,
    /// Op index of the deferred (wedging) op — the arbitration's `at_op`.
    first_op: usize,
    /// Consecutive `RecoveryRequired` refusals naming `operation_id`.
    streak: usize,
    /// The wedging op whose `reconcile_after_failure` the watch withheld.
    deferred_wop: WorldOp,
}

/// The interrupting event at a keep-serving resolution: the op whose
/// outcome ended the watch — the second unjudged op the widened
/// arbitration exists for ([`reconcile_watch_resolution`]).
struct WatchInterrupt<'a> {
    wop: &'a WorldOp,
    /// true = E is known applied (a success ended the watch — the healed
    /// composition): the E-absent hypotheses are impossible and dropped.
    /// false = E failed/died with possibly-durable effects: its fate is
    /// judged here alongside A's.
    applied: bool,
}

/// The standings one op can take inside a composition: `Applied`, the
/// `LoadFork` fork-survives half-state, `NotApplied`.
fn op_modes(wop: &WorldOp) -> Vec<ReconcileOutcome> {
    let mut modes = vec![ReconcileOutcome::Applied];
    if matches!(wop, WorldOp::LoadFork { .. }) {
        modes.push(ReconcileOutcome::ForkOnly);
    }
    modes.push(ReconcileOutcome::NotApplied);
    modes
}

/// One composition hypothesis: what the deferred op (A) and the
/// interrupting op (E) did, in which order, with the model and render that
/// history produces.
struct CompositionHypothesis {
    a: ReconcileOutcome,
    e: ReconcileOutcome,
    /// Order: E composed BEFORE A. A distinct hypothesis exactly because
    /// state-derived ops (`BranchCreate`, `LoadFork`, `BranchMerge`) read
    /// branch state at their moment — `A+E` and `E+A` render differently
    /// when E forks a branch A's effect lives on (specimen seed 24).
    e_first: bool,
    world: WorldModel,
    render: WorldState,
}

impl CompositionHypothesis {
    /// Report provenance: which composition this is, e.g. `A+E`, `E`,
    /// `fork(A)+E`, `none`.
    fn desc(&self, has_interrupt: bool) -> String {
        let name = |m: ReconcileOutcome, tag: &str| match m {
            ReconcileOutcome::NotApplied => None,
            ReconcileOutcome::ForkOnly => Some(format!("fork({tag})")),
            ReconcileOutcome::Applied => Some(tag.to_string()),
        };
        let a = name(self.a, "A");
        let e = if has_interrupt {
            name(self.e, "E")
        } else {
            None
        };
        let parts: Vec<String> = if self.e_first {
            [e, a].into_iter().flatten().collect()
        } else {
            [a, e].into_iter().flatten().collect()
        };
        if parts.is_empty() {
            "none".to_string()
        } else {
            parts.join("+")
        }
    }
}

/// Can `wop` be applied to this model at all? Guards every panic
/// `apply_world` can raise ("live branch" on `Data`, the `BranchMerge`
/// source index, `BranchDelete` of an absent name) plus the create dual:
/// a `BranchCreate` of a name the model already holds is physically
/// impossible (the engine refuses an existing name), and building it
/// would overwrite the slot and rule provenance on a phantom composition.
/// A composition ORDER can legitimately produce any of these states —
/// e.g. E-first `BranchDelete b0` followed by A on `b0` — and such an
/// order is structurally impossible, not a bug: the hypothesis is
/// dropped, never built. Scope-out: a `LoadFork` applied after its branch
/// already exists models the engine's load-into-existing path as
/// fork-plus-load (slot overwrite) — a semantic approximation, kept
/// because excluding the order could drop the true composition.
fn op_targets_live(world: &WorldModel, wop: &WorldOp) -> bool {
    match wop {
        WorldOp::Data { branch, .. } => world.state_of_opt(branch).is_some(),
        WorldOp::BranchMerge { source } => world.branches.contains_key(source),
        WorldOp::BranchDelete { name } => world.branches.contains_key(name),
        WorldOp::BranchCreate { name } => !world.branches.contains_key(name),
        WorldOp::LoadFork { .. } => true,
    }
}

/// Render every legal composition of the deferred op A and (when present)
/// the interrupting op E from the CURRENT model: for each combination of
/// standings, clone the model, apply the ops in the composition's order,
/// render. Orders whose next op targets a branch state that makes it
/// impossible are skipped ([`op_targets_live`]). Sorted most-applied-first
/// (A's standing, then E's, then A-first order — `ReconcileOutcome`'s
/// `Ord`) so first-match preference mirrors [`reconcile_after_failure`]'s
/// `Applied`-before-`ForkOnly`-before-`NotApplied` outcome order. With no
/// interrupt this is exactly the one-op set {applied, (fork-only,)
/// absent}.
fn composition_hypotheses(
    world: &WorldModel,
    deferred: &WorldOp,
    interrupt: Option<&WatchInterrupt<'_>>,
) -> Vec<CompositionHypothesis> {
    let a_modes = op_modes(deferred);
    let e_modes = match interrupt {
        None => vec![ReconcileOutcome::NotApplied],
        Some(i) if i.applied => vec![ReconcileOutcome::Applied],
        Some(i) => op_modes(i.wop),
    };
    let mut hyps = Vec::new();
    for &a in &a_modes {
        for &e in &e_modes {
            let orders: &[bool] =
                if a != ReconcileOutcome::NotApplied && e != ReconcileOutcome::NotApplied {
                    &[false, true]
                } else {
                    &[false]
                };
            'order: for &e_first in orders {
                let mut w = world.clone();
                let seq: [(Option<&WorldOp>, ReconcileOutcome); 2] = if e_first {
                    [(interrupt.map(|i| i.wop), e), (Some(deferred), a)]
                } else {
                    [(Some(deferred), a), (interrupt.map(|i| i.wop), e)]
                };
                for (op, mode) in seq {
                    let Some(op) = op else { continue };
                    if mode == ReconcileOutcome::NotApplied {
                        continue;
                    }
                    if !op_targets_live(&w, op) {
                        continue 'order;
                    }
                    mode.apply(&mut w, op);
                }
                let render = w.render();
                hyps.push(CompositionHypothesis {
                    a,
                    e,
                    e_first,
                    world: w,
                    render,
                });
            }
        }
    }
    hyps.sort_by(|x, y| {
        y.a.cmp(&x.a)
            .then(y.e.cmp(&x.e))
            .then(x.e_first.cmp(&y.e_first))
    });
    hyps
}

/// The widened resolution's verdict: both ops' outcomes, the matched
/// composition (report provenance), and that composition's model — the
/// matching composition becomes the model, wholesale.
struct WatchRuling {
    a_outcome: ReconcileOutcome,
    /// `Some` exactly when an interrupt was passed. CANONICAL exactly-once
    /// contract: the interrupting op's judgment is FINAL here and the call
    /// site MUST NOT judge it again — the resolution's reopen empties
    /// `__recovery/` of every recognized-name strand (the tolerated
    /// foreign-named carve-out is an injected misdirect, never an op's own
    /// strand), so nothing survives to change the op's fate.
    e_outcome: Option<ReconcileOutcome>,
    matched: String,
    world: WorldModel,
}

/// The keep-serving resolution's arbitration (the #559 composition
/// widening; regression evidence in
/// `dst_keep_serving_widened_arbitration_no_false_reds`): judge the
/// deferred op A and the interrupting op E TOGETHER, against every legal
/// composition and order of the pair. [`reconcile_after_failure`]'s one-op
/// set assumes at most one unjudged op separates model from store; the
/// watch's deferral breaks that invariant — the regression test's doc
/// carries the three proven break shapes. Red only when NO composition
/// matches; the matching composition becomes the model.
///
/// Same six steps as [`reconcile_after_failure`] — look, reopen
/// ([`reopen_under_storm`]), look again, residue
/// ([`assert_no_recovery_residue`]), monotonicity, rule — with the checks
/// generalized to the widened set: a fact every pre-reopen match agrees on
/// must survive recovery, and render ties whose models differ resolve
/// through the physical channel like the one-op ghost tie-break.
#[allow(clippy::too_many_arguments)]
async fn reconcile_watch_resolution(
    db: Omnigraph,
    storage: Arc<dyn StorageAdapter>,
    root: &str,
    deferred: &WorldOp,
    interrupt: Option<&WatchInterrupt<'_>>,
    world: &WorldModel,
    label: &str,
    at_op: usize,
) -> (Omnigraph, WatchRuling, &'static str) {
    let hyps = composition_hypotheses(world, deferred, interrupt);
    // The failure carries its own triage: a no-match red prints every
    // candidate composition it compared, so the reader can diff instead of
    // re-deriving (the #559 root-cause lesson — with the renders in the
    // message, diagnosis took two runs; without, a dedicated session).
    let candidates = |hyps: &[CompositionHypothesis]| {
        hyps.iter()
            .map(|h| format!("{}={:?}", h.desc(interrupt.is_some()), h.render))
            .collect::<Vec<_>>()
            .join("; ")
    };
    let matches_of = |state: &WorldState| -> Vec<usize> {
        hyps.iter()
            .enumerate()
            .filter(|(_, h)| h.render == *state)
            .map(|(i, _)| i)
            .collect()
    };
    let visible = observe_world(&db).await;
    let visible_matches = matches_of(&visible);
    if visible_matches.is_empty() {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!(
                "{label}: PARTIAL application (deferred={deferred:?}, interrupt={:?}); \
                 visible={visible:?}; candidates: {}",
                interrupt.map(|i| i.wop),
                candidates(&hyps)
            ),
            "the pre-reopen world renders as a legal composition of the unjudged ops",
        );
    }
    drop(db);
    // `recovery_crash: None` — the double-fault lever is deliberately not
    // exercised at watch resolutions (parity with the kill/fault reconcile
    // sites; the crash-window arm resolves BEFORE `crash_op`, so the lever
    // still fires on that crash's own reconcile). A keep-serving ×
    // double-fault arm is recorded future work.
    let db = reopen_under_storm(&storage, root, label, at_op, None).await;
    let after = observe_world(&db).await;
    let mut after_matches = matches_of(&after);
    if after_matches.is_empty() {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!(
                "{label}: recovery produced an illegal state (deferred={deferred:?}, \
                 interrupt={:?}); after={after:?}; candidates: {}",
                interrupt.map(|i| i.wop),
                candidates(&hyps)
            ),
            "the post-recovery world renders as a legal composition of the unjudged ops",
        );
    }
    assert_no_recovery_residue(&storage, root, label, at_op).await;
    // Ambiguity: several compositions can render identically while their
    // MODELS differ in ghost content. Resolve through the physical channel
    // per touched branch; a raw read matching NO tied composition is its
    // own violation. Runs BEFORE the monotonicity checks so they judge the
    // narrowed set — quantifying over pre-narrowing ties could let the
    // tie-break eliminate the only Applied match after a demotion check
    // already passed, installing a demoted model with no red. Scope:
    // `touched` covers the `Data` branches of A and E only — a ghost
    // divergence born inside a `LoadFork`'s fork copy or a `BranchMerge`'s
    // ghost import falls to preference order. Acceptable while ghosts are
    // empty by construction (post-#474; the ghost set is a regression
    // tripwire), and inherited from [`reconcile_after_failure`]'s identical
    // Data-only tie-break scope. Ties whose models are ghost-identical fall
    // to preference order — the models being equal, the pick is immaterial.
    let mut channel: &'static str = "query";
    if after_matches.len() > 1 {
        let mut touched: Vec<&String> = Vec::new();
        if let WorldOp::Data { branch, .. } = deferred {
            touched.push(branch);
        }
        if let Some(i) = interrupt
            && let WorldOp::Data { branch, .. } = i.wop
        {
            touched.push(branch);
        }
        for branch in touched {
            // The raw expectation per tied composition: edges ∪ ghosts on
            // the touched branch (None = branch absent in that model —
            // uniform across ties, since the shared render lists branches).
            let expectations: Vec<Option<Vec<(String, String)>>> = after_matches
                .iter()
                .map(|&idx| {
                    hyps[idx]
                        .world
                        .state_of_opt(branch)
                        .map(Model::edges_with_ghosts)
                })
                .collect();
            let first = &expectations[0];
            if first.is_none() || expectations.iter().all(|e| e == first) {
                continue;
            }
            channel = "query+physical";
            let (_, knows) = Box::pin(physical_view_on(&db, branch)).await;
            let keep: Vec<usize> = after_matches
                .iter()
                .zip(&expectations)
                .filter(|(_, e)| e.as_deref() == Some(knows.as_slice()))
                .map(|(&idx, _)| idx)
                .collect();
            if keep.is_empty() {
                detectors::violation(
                    DET_ARBITRATION_PHYSICAL,
                    at_op,
                    format!(
                        "{label}: physical channel matches NO tied composition on \
                         '{branch}' (physical={knows:?}; tied expectations: {:?})",
                        expectations
                    ),
                    "the export tie-break resolves ghost-only differences to one composition",
                );
            }
            after_matches = keep;
        }
    }
    // Monotonicity across the widened set, judged on the NARROWED matches:
    // a fact EVERY pre-reopen match agrees on must not be undone by
    // recovery. Quantified over all matches ([`reconcile_after_failure`]
    // uses exact equality on its single `as_with`) so render ambiguity
    // never manufactures a false demotion.
    let demoted = |get: &dyn Fn(&CompositionHypothesis) -> ReconcileOutcome| {
        visible_matches
            .iter()
            .all(|&i| get(&hyps[i]) == ReconcileOutcome::Applied)
            && !after_matches
                .iter()
                .any(|&i| get(&hyps[i]) == ReconcileOutcome::Applied)
    };
    // Fork-survives oracle, both operands: the implicit fork is a fully
    // published branch create; recovery must never delete it (it may still
    // roll the LOAD forward).
    let fork_deleted = |get: &dyn Fn(&CompositionHypothesis) -> ReconcileOutcome| {
        visible_matches
            .iter()
            .all(|&i| get(&hyps[i]) != ReconcileOutcome::NotApplied)
            && after_matches
                .iter()
                .all(|&i| get(&hyps[i]) == ReconcileOutcome::NotApplied)
    };
    if demoted(&|h| h.a) {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!(
                "{label}: recovery DEMOTED a committed write (deferred={deferred:?}); after={after:?}"
            ),
            "recovery monotonicity: a committed write stays applied",
        );
    }
    if matches!(deferred, WorldOp::LoadFork { .. }) && fork_deleted(&|h| h.a) {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!(
                "{label}: recovery DELETED a durably created implicit fork branch \
                 (deferred={deferred:?}); after={after:?}"
            ),
            "recovery monotonicity: a durably created fork branch survives recovery",
        );
    }
    if let Some(i) = interrupt
        && !i.applied
    {
        if matches!(i.wop, WorldOp::LoadFork { .. }) && fork_deleted(&|h| h.e) {
            detectors::violation(
                DET_CRASH_CONTRACT,
                at_op,
                format!(
                    "{label}: recovery DELETED the interrupting op's durably created \
                     implicit fork branch (interrupt={:?}); after={after:?}",
                    i.wop
                ),
                "recovery monotonicity: a durably created fork branch survives recovery",
            );
        }
        if demoted(&|h| h.e) {
            detectors::violation(
                DET_CRASH_CONTRACT,
                at_op,
                format!(
                    "{label}: recovery DEMOTED the interrupting op's committed write (interrupt={:?}); after={after:?}",
                    i.wop
                ),
                "recovery monotonicity: a committed write stays applied",
            );
        }
    }
    let winner = &hyps[after_matches[0]];
    let ruling = WatchRuling {
        a_outcome: winner.a,
        e_outcome: interrupt.map(|_| winner.e),
        matched: winner.desc(interrupt.is_some()),
        world: winner.world.clone(),
    };
    (db, ruling, channel)
}

/// Resolve a keep-serving watch: push the site's `known_issues` row
/// (`<site-prefix>op<i>:<id> after N refusals (first at opK)`), run the
/// DEFERRED arbitration ([`reconcile_watch_resolution`]) — widened with
/// the interrupting op E when one exists — and install the matched
/// composition as the model.
///
/// CANONICAL, the deferral contract: the watch withholds only the REOPEN,
/// so resolution must run before any other reconcile can reopen, or the
/// model would carry an unjudged effect window through Full recovery.
///
/// CANONICAL, the stale-capture rule: this resolution replaces the model
/// and reopens the store, so any predicate captured before it (a merge
/// prediction, a damage snapshot) must be re-derived — or deliberately
/// snapshotted pre-resolution — before judging THIS iteration's op
/// against the post-ruling world.
///
/// The returned interrupt outcome is final — exactly-once contract on
/// [`WatchRuling::e_outcome`]. Violations inside carry the DEFERRED op's
/// index as `at_op`; the interrupt's identity is in the message.
/// Mid-watch history stays coherent without an assert: a pending strand's
/// head is un-advanced (a partial multi-table commit never advances the
/// manifest head), and every head-advancing path (an entry heal) resolves
/// the watch within its own iteration before the next capture. No
/// `maintenance_obligations` pass runs here — parity with the
/// injected-fault reconcile path, which never ran one either; a deferred
/// MAINTENANCE op that executed and armed its own strand gets no
/// obligations judgment on this path — future work; crash/kill deaths
/// remain the only obligation triggers.
#[allow(clippy::too_many_arguments)]
async fn resolve_keep_serving_watch(
    db: Omnigraph,
    storage: Arc<dyn StorageAdapter>,
    root: &str,
    failing: Option<&FailingStorage>,
    world: &mut WorldModel,
    reconcile_verdicts: &mut Vec<(String, String, String)>,
    known_issues: &mut Vec<String>,
    row_site: &str,
    watch: KeepServingWatch,
    interrupt: Option<&WatchInterrupt<'_>>,
) -> (Omnigraph, Option<(ReconcileOutcome, &'static str)>) {
    known_issues.push(format!(
        "{row_site}:{} after {} refusals (first at op{})",
        watch.operation_id, watch.streak, watch.first_op
    ));
    if let Some(f) = failing {
        f.suspend();
    }
    let (db, ruling, channel) = Box::pin(reconcile_watch_resolution(
        db,
        storage,
        root,
        &watch.deferred_wop,
        interrupt,
        world,
        "keep-serving deferred arbitration",
        watch.first_op,
    ))
    .await;
    if let Some(f) = failing {
        f.resume();
    }
    reconcile_verdicts.push((
        format!("keep-serving-deferred@op{}", watch.first_op),
        format!("{:?} matched={}", ruling.a_outcome, ruling.matched),
        channel.to_string(),
    ));
    *world = ruling.world;
    (db, ruling.e_outcome.map(|o| (o, channel)))
}

pub fn run_universe(root: &str, sc: &Scenario) -> UniverseReport {
    match run_universe_caught(root, sc) {
        Ok(report) => report,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

/// META ORACLE — strict replay, detector-tagged: two same-seed
/// reports must be equal, row order included. The pins' comparison funnel;
/// a red names (HarnessOutput, StrictReplay) in its recorded row.
pub fn assert_strict_replay(first: &UniverseReport, second: &UniverseReport, context: &str) {
    if first != second {
        detectors::violation(
            DET_REPLAY,
            0,
            format!("{context}: same-seed reports differ\nfirst:  {first:?}\nsecond: {second:?}"),
            "the same seed reproduces an equal UniverseReport, row order included",
        );
    }
}

/// Render a caught universe panic as a repro-bearing message. MAY SPAN
/// LINES (a violation's `observed` can embed newlines — e.g. the strict
/// replay diff); the fleet's FAILURE_JSON row is the single-line form.
/// Detector-tagged violations render their `detector=` field —
/// the fleet's failure records carry it without any caller change.
pub fn panic_message(panic: &(dyn std::any::Any + Send)) -> String {
    if let Some(v) = panic.downcast_ref::<crate::detectors::Violation>() {
        v.render()
    } else if let Some(s) = panic.downcast_ref::<String>() {
        s.clone()
    } else if let Some(s) = panic.downcast_ref::<&str>() {
        (*s).to_string()
    } else {
        "<non-string panic payload>".to_string()
    }
}

/// the fleet's universe entry: a VIOLATION becomes a recorded
/// row instead of killing the whole pass. Identical execution to
/// `run_universe` (same dedicated 16 MiB thread — its join already carries
/// the panic; the pinned-suite path rethrows, this path returns it). The
/// caller records (scenario, message) — a complete repro — and continues.
pub fn run_universe_caught(
    root: &str,
    sc: &Scenario,
) -> Result<UniverseReport, Box<dyn std::any::Any + Send>> {
    // Seed logging: the knobs the pinned scenarios vary, so a failed run's
    // line names its universe (remaining Scenario knobs come from the
    // test's own source).
    println!(
        "dst universe [root={root} seed={} ops={} crash={:?} crash_on_match={:?} faults={} kill={:?} keep_serving={}]",
        sc.seed,
        sc.ops,
        sc.crash_at,
        sc.crash_on_match,
        sc.faults
            .as_ref()
            .map(|p| format!(
                "seed={} error_pct={} lance_realm={}",
                p.seed, p.error_pct, p.lance_realm
            ))
            .unwrap_or_else(|| "none".to_string()),
        sc.die_at_write,
        sc.keep_serving_ops
    );
    // Structural scope-out: the ack-loss client-retry re-executes an op the
    // keep-serving machinery may have already judged at a watch resolution —
    // the combination is undesigned. Enforced here, not by comment alone.
    assert!(
        sc.keep_serving_ops == 0 || !sc.faults.as_ref().map(|p| p.client_retry).unwrap_or(false),
        "keep_serving_ops and FaultPlan::client_retry are mutually scoped out"
    );

    let mut seeds = SplitMix64(sc.seed);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    let workload_seed = seeds.next_u64();
    let entropy_seed = seeds.next_u64();

    detectors::install_violation_panic_hook();
    clear_process_slots();
    crate::env_knobs::require_pool_env();

    // Lance's retry backoff draws REAL entropy (the REPLAY-ENVELOPE NOTE on
    // `UniverseReport`). Close what is closable in-process: (re)arm the
    // entropy shim with a per-universe stream and force this thread's
    // ThreadRng to re-pull from it — every jitter draw becomes a
    // deterministic function of the universe seed, no matter what earlier
    // universes consumed.
    crate::entropy::arm(entropy_seed);

    // THE UNIVERSE THREAD: every universe runs on a dedicated 16 MiB thread
    // — the systemic fix for the 2 MiB test stack (supersedes per-frame
    // Box::pin whack-a-mole). ThreadRng is THREAD-LOCAL, so the
    // per-universe entropy reseed must happen INSIDE this thread; panics
    // (oracle verdicts) propagate via resume_unwind so test messages
    // survive. One fresh thread per universe keeps replay exact.
    let result = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("dst-universe".into())
            .stack_size(UNIVERSE_STACK_BYTES)
            .spawn_scoped(scope, || {
                let _ = rand::rng().reseed();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .start_paused(true)
                    .rng_seed(tokio::runtime::RngSeed::from_bytes(
                        &runtime_seed.to_le_bytes(),
                    ))
                    .build_local(Default::default())
                    .expect("seeded current-thread runtime");

    // The WHOLE universe future is heap-allocated: the
    // accumulated session/oracle state pushed the inline state machine past
    // the 2 MiB test stack — boxing at the outermost boundary composes with
    // the 16 MiB universe thread above.
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();

        // The concrete handle survives beside the dyn one so the write
        // census can bottom-list the store itself at universe end
        // (`dst_list_all_keys` is inherent, not on the trait).
        let concrete_adapter = Arc::new(ObjectStorageAdapter::in_memory());
        let base: Arc<dyn StorageAdapter> = concrete_adapter.clone();
        // Bench counting pass: when a cost ledger is armed, count at the
        // innermost adapter layer (exactly the calls that reach the store)
        // and make sure the Lance-realm decorator is interposed so its
        // tallies fire too. Disarmed: zero change.
        let base: Arc<dyn StorageAdapter> = if crate::cost::armed() {
            crate::lance_faults::install();
            Arc::new(crate::cost::CostStorage::new(base))
        } else {
            base
        };
        // the same plan storms BOTH realms — the adapter realm via
        // FailingStorage, the Lance realm via the interposed provider. The
        // slot is set unconditionally so a panicked faulty universe cannot
        // leak weather into the next universe in this process.
        let lance_faults_state = sc
            .faults
            .as_ref()
            .filter(|plan| plan.lance_realm)
            .map(crate::lance_faults::LanceFaultState::from_plan);
        // the crash-state enumeration needs the same wrapped storage (its
        // write counter lives in the wrappers of both realms).
        let kill_state = sc.die_at_write.map(KillState::new);
        if lance_faults_state.is_some() || kill_state.is_some() {
            crate::lance_faults::install();
        }
        crate::lance_faults::set_active(lance_faults_state.clone());
        crate::lance_faults::set_kill(kill_state.clone());
        // Persisted tier: per-universe sink for the foreign-sidecar carve-out
        // rows — cleared unconditionally (same leak-safety rule as the
        // lance slots: a panicked predecessor must not bleed rows in).
        FOREIGN_SIDECAR_ROWS.lock().unwrap().clear();
        let failing: Option<Arc<FailingStorage>> = if sc.faults.is_some() || kill_state.is_some() {
            Some(Arc::new(FailingStorage::new(
                base.clone(),
                sc.faults.clone().unwrap_or_else(FaultPlan::none),
                lance_faults_state.clone(),
                kill_state.clone(),
            )))
        } else {
            None
        };
        let storage: Arc<dyn StorageAdapter> = match &failing {
            Some(f) => f.clone(),
            None => base,
        };

        let mut db = Omnigraph::init_with_storage(
            root,
            TEST_SCHEMA,
            storage.clone(),
            InitOptions::default(),
        )
        .await
        .expect("init universe root");
        load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
            .await
            .expect("load fixture");

        // Model = the loaded fixture, edges included; branch-aware world.
        let mut world = WorldModel::default();
        for (name, age, ver) in person_rows(&db).await {
            world.main.persons.insert(name, (age, ver));
        }
        for pair in knows_pairs(&db).await {
            world.main.edges.insert(pair);
        }
        // history baseline (the fixture-load commit) — captured
        // before fault injection enables so the baseline read is clean.
        let mut history: Vec<(String, Model)> = Vec::new();
        let mut history_verified_from: usize = 0;
        Box::pin(capture_history(&db, &world.main, &mut history)).await;

        // the BYSTANDER session — born now (clean store), never
        // writes, read at every session check. The server's warm-idle shape.
        let bystander = Box::pin(Omnigraph::open_with_storage(root, storage.clone()))
            .await
            .expect("open bystander session");
        let mut bystander_last: Option<usize> = None;
        let mut bystander_trail: Vec<usize> = Vec::new();
        let mut session_checks = 0usize;
        let mut force_session_check = false;

        if let Some(f) = &failing {
            f.enable();
        }

        let mut rng = SplitMix64(workload_seed);
        let mut next_ver: i64 = 0;
        let mut schema_extras = 0usize;
        let mut fresh_load = 0usize;
        // milestone plan for a reach target (empty otherwise).
        let reach_steps: Vec<Milestone> = sc.reach_target.map(milestone_steps).unwrap_or_default();
        let mut reach_progress = 0usize;
        let mut crashes = 0usize;
        let mut verified = 0usize;
        let mut legal_rejections = 0usize;
        let mut client_retries = 0usize;
        let mut maintenance_reruns = 0usize;
        let mut reconcile_verdicts: Vec<(String, String, String)> = Vec::new();
        let mut known_issues: Vec<String> = Vec::new();
        // Armed only when `Scenario::keep_serving_ops > 0`; contract on
        // [`KeepServingWatch`].
        let mut keep_serving_watch: Option<KeepServingWatch> = None;
        // attributed detections — op failures whose reads
        // crossed the damage ledger (see the exec-site snapshot below).
        let mut corruption_detections: Vec<String> = Vec::new();
        // Targeted-scheduling state: pending until the (skip+1)-th
        // family-matching op has been crash-executed (or probe-executed).
        let (mut match_pending, mut match_skip) = match sc.crash_on_match {
            Some((_, skip)) => (true, skip),
            None => (false, 0),
        };
        let crossed_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        // The persistent record-only census probe over the whole
        // universe — recoveries and audits included. Held to universe end.
        #[cfg(feature = "failpoints")]
        let _persistent_probe = sc.probe_window.map(|w| {
            let flag = crossed_flag.clone();
            omnigraph::failpoints::ScopedFailPoint::with_callback(w, move || {
                flag.store(true, std::sync::atomic::Ordering::SeqCst)
            })
        });

        for i in 0..sc.ops {
            // record the PREVIOUS op's head advance (oracle read,
            // fault-suspended; consecutive same-head captures dedupe).
            if !sc.ablate_history {
                if let Some(f) = &failing {
                    f.suspend();
                }
                crate::cost::set_label("_history");
                Box::pin(capture_history(&db, &world.main, &mut history)).await;
                if let Some(f) = &failing {
                    f.resume();
                }
            }

            // Emit any due milestone op (see `Scenario::reach_target`),
            // else fall through to ordinary sampling.
            let wop = milestone_op(
                &reach_steps,
                &mut reach_progress,
                sc.ops - i,
                &mut rng,
                &world,
                &mut next_ver,
            )
            .unwrap_or_else(|| {
                sample_world_op(
                    &mut rng,
                    &world,
                    &mut next_ver,
                    sc.hostile,
                    sc.wide,
                    &mut schema_extras,
                    &mut fresh_load,
                )
            });
            // RETENTION HORIZON: `cleanup(keep_versions: 1)`
            // retires old table versions, so history recorded before a
            // cleanup is no longer RELIABLY readable (GC is lazy and
            // per-table — older entries are neither provably readable nor
            // provably gone). Truncate the verified history to the current
            // head instead of guessing; conservative on any cleanup ATTEMPT
            // (a crashed cleanup may have GC'd partially). Found by the
            // oracle's own first run. NOTE (issue candidate, Azim judges): a
            // time-travel read below the horizon surfaces a raw UNTYPED
            // Lance "Dataset ... manifest was not found" — no typed
            // retention error, nothing names cleanup as the cause.
            if matches!(
                &wop,
                WorldOp::Data {
                    op: Op::Cleanup,
                    ..
                }
            ) {
                // Marker, not a drain: the
                // history oracle re-reads STORAGE and must respect the
                // horizon, but the bystander-membership check is pure
                // model-side — retention can't invalidate it, so the full
                // list stays.
                history_verified_from = history.len().saturating_sub(1);
            }

            let expected_conflict = expects_merge_conflict(&world, &wop);
            // Debug aid: DST_OP_LOG=1 prints each sampled op — turns any
            // failing universe's seed line into a full repro transcript
            // (this is how the version collision and the liveness-oracle bug were
            // minimized).
            if crate::harness::debug_knobs::OP_LOG.load(std::sync::atomic::Ordering::Relaxed)
                || std::env::var("DST_OP_LOG").is_ok()
            {
                println!("  op[{i}] conflict_predicted={expected_conflict} {wop:?}");
            }
            // Bench counting pass: attribute the coming storage actions to
            // this op kind (no-op when the cost ledger is disarmed).
            crate::cost::set_label(&crate::cost::debug_head(&wop));

            // Unified crash trigger: blind index (`crash_at`) or targeted
            // family match (`crash_on_match`) — at most one crash either way.
            let mut crash_now: Option<&'static str> = None;
            if let Some((crash_idx, failpoint)) = sc.crash_at {
                crash_now = (i == crash_idx).then_some(failpoint);
            }
            if crash_now.is_none() && match_pending {
                let (failpoint, _) = sc.crash_on_match.expect("pending implies present");
                if window_matches(failpoint, &wop) {
                    if match_skip == 0 {
                        match_pending = false;
                        crash_now = Some(failpoint);
                    } else {
                        match_skip -= 1;
                    }
                }
            }
            if let Some(failpoint) = crash_now.filter(|_| !sc.probe_only) {
                // A scheduled crash ends any keep-serving experiment first —
                // deferral contract on [`resolve_keep_serving_watch`]. No
                // interrupt: the crashing op has not executed yet, so it
                // cannot be in the store.
                let mut expected_conflict = expected_conflict;
                if let Some(watch) = keep_serving_watch.take() {
                    let (new_db, _) = Box::pin(resolve_keep_serving_watch(
                        db,
                        storage.clone(),
                        root,
                        failing.as_deref(),
                        &mut world,
                        &mut reconcile_verdicts,
                        &mut known_issues,
                        &format!("{KEEP_SERVING_INTERRUPTED_PREFIX}op{i}"),
                        watch,
                        None,
                    ))
                    .await;
                    db = new_db;
                    // Stale-capture rule on [`resolve_keep_serving_watch`]:
                    // re-derive the prediction from the world crash_op will
                    // actually judge against.
                    expected_conflict = expects_merge_conflict(&world, &wop);
                }
                let (new_db, outcome) = Box::pin(crash_op(
                    db,
                    storage.clone(),
                    root,
                    &wop,
                    failpoint,
                    &world,
                    i,
                    sc.recovery_crash,
                    expected_conflict,
                    failing.as_deref(),
                ))
                .await;
                db = new_db;
                match outcome {
                    CrashOutcome::OpSucceeded => apply_world(&mut world, &wop),
                    CrashOutcome::LegalRejection => legal_rejections += 1,
                    CrashOutcome::Crashed { outcome, channel } => {
                        crashes += 1;
                        reconcile_verdicts.push((
                            format!("crash:{failpoint}@op{i}"),
                            format!("{outcome:?}"),
                            channel.to_string(),
                        ));
                        outcome.apply(&mut world, &wop);
                        // a maintenance death gets its
                        // obligations judged, fault-suspended. Gated on the
                        // target still existing: a same-iteration ruling can
                        // remove it, and the convergence rerun would fail
                        // "not found" — a false red, not a divergence.
                        if let Some(f) = &failing {
                            f.suspend();
                        }
                        let ran = if op_targets_live(&world, &wop) {
                            Box::pin(maintenance_obligations(
                                &mut db,
                                &world,
                                &wop,
                                &format!("crash:{failpoint}@op{i}"),
                                i,
                                sc.fail_maintenance_rerun,
                            ))
                            .await
                        } else {
                            false
                        };
                        if let Some(f) = &failing {
                            f.resume();
                        }
                        if ran {
                            maintenance_reruns += 1;
                            if matches!(
                                &wop,
                                WorldOp::Data {
                                    op: Op::Cleanup,
                                    ..
                                }
                            ) {
                                // The rerun is a cleanup ATTEMPT — same
                                // retention-horizon truncation as mainline.
                                history_verified_from = history.len().saturating_sub(1);
                            }
                        }
                    }
                }
                continue;
            }

            // attribution window: snapshot the damage ledger's
            // event counter around the op. Corruption-born engine errors are
            // unmarkable by construction (the error is the ENGINE's own
            // detection, born from parsing our damaged bytes), so
            // attribution is by overlap: the counter advanced during the op
            // iff at least one of its reads was damaged. Oracle windows are
            // fault-suspended, so between-op reads never advance it.
            let damage_before = failing.as_ref().map(|f| f.damage_events()).unwrap_or(0);

            // Crossing probe: install a record-only callback over exactly the
            // matched op, then run it through the NORMAL path — the guard
            // drops right after the op so verification reads can't blur the
            // attribution.
            #[cfg(feature = "failpoints")]
            let exec_result = {
                let _probe_guard = match crash_now {
                    Some(failpoint) if sc.probe_only => {
                        let flag = crossed_flag.clone();
                        Some(omnigraph::failpoints::ScopedFailPoint::with_callback(
                            failpoint,
                            move || flag.store(true, std::sync::atomic::Ordering::SeqCst),
                        ))
                    }
                    _ => None,
                };
                exec_world_op(&mut db, &wop).await
            };
            #[cfg(not(feature = "failpoints"))]
            let exec_result = exec_world_op(&mut db, &wop).await;

            // the dead flag — not the error text, not even the
            // op's verdict — is the authority. The dying op may surface a
            // LATER internal write's post-mortem error instead of the fatal
            // write's, or ABSORB the loss entirely and claim success (a
            // best-effort write, the reopen-heals class — observed on the
            // first enumeration run: k=1 landed in a swallowed write and the op
            // returned Ok). Either way: revive (the process-restart analog),
            // disarm (one death per universe, like crash windows), and let
            // reconcile arbitrate what actually landed.
            let killed_now = kill_state.as_ref().map(|s| s.dead()).unwrap_or(false);
            if killed_now {
                let ks = kill_state.as_ref().expect("dead implies kill state");
                println!(
                    "dst crash state: write #{} ({}) {} op {:?}",
                    ks.writes_observed(),
                    ks.killed_label().unwrap_or_default(),
                    if exec_result.is_ok() {
                        "ABSORBED by"
                    } else {
                        "killed"
                    },
                    wop
                );
                if exec_result.is_err() {
                    legal_rejections += 1;
                }
                ks.revive_and_disarm();
                // Deferral contract on [`resolve_keep_serving_watch`].
                // Unlike the crash-window arm, the dying op EXECUTED (the
                // kill fired mid-op), so it rides into the resolution as the
                // uncertain interrupting op — DELIBERATELY uncertain even on
                // the ABSORBED `Ok`: here the dead flag, not the op's claim,
                // is the authority, so a kill-context success is exactly the
                // claim the arbitration must not trust.
                let interrupt_ruling = if let Some(watch) = keep_serving_watch.take() {
                    let interrupt = WatchInterrupt {
                        wop: &wop,
                        applied: false,
                    };
                    let (new_db, ruling) = Box::pin(resolve_keep_serving_watch(
                        db,
                        storage.clone(),
                        root,
                        failing.as_deref(),
                        &mut world,
                        &mut reconcile_verdicts,
                        &mut known_issues,
                        &format!("{KEEP_SERVING_INTERRUPTED_PREFIX}op{i}"),
                        watch,
                        Some(&interrupt),
                    ))
                    .await;
                    db = new_db;
                    ruling
                } else {
                    None
                };
                let (outcome, channel) = if let Some((outcome, channel)) = interrupt_ruling {
                    // Exactly-once contract on [`WatchRuling::e_outcome`] —
                    // row only.
                    (outcome, channel)
                } else {
                    if let Some(f) = &failing {
                        f.suspend();
                    }
                    let (new_db, outcome, channel) = Box::pin(reconcile_after_failure(
                        db,
                        storage.clone(),
                        root,
                        &wop,
                        &world,
                        "crash-state death",
                        i,
                        None,
                    ))
                    .await;
                    db = new_db;
                    if let Some(f) = &failing {
                        f.resume();
                    }
                    outcome.apply(&mut world, &wop);
                    (outcome, channel)
                };
                reconcile_verdicts.push((
                    format!("crash-state:write#{}@op{i}", ks.writes_observed()),
                    format!("{outcome:?}"),
                    channel.to_string(),
                ));
                // a maintenance death gets its obligations
                // judged, fault-suspended. Gated on the target still
                // existing — same rule as the crash-window arm: a ruling
                // can remove it, and the rerun would false-red "not found".
                if let Some(f) = &failing {
                    f.suspend();
                }
                let ran = if op_targets_live(&world, &wop) {
                    Box::pin(maintenance_obligations(
                        &mut db,
                        &world,
                        &wop,
                        &format!("crash-state:write#{}@op{i}", ks.writes_observed()),
                        i,
                        sc.fail_maintenance_rerun,
                    ))
                    .await
                } else {
                    false
                };
                if let Some(f) = &failing {
                    f.resume();
                }
                if ran {
                    maintenance_reruns += 1;
                    if matches!(
                        &wop,
                        WorldOp::Data {
                            op: Op::Cleanup,
                            ..
                        }
                    ) {
                        history_verified_from = history.len().saturating_sub(1);
                    }
                }
            } else {
                match exec_result {
                    Ok(()) => {
                        // A merge the model predicted as conflicting MUST NOT
                        // succeed — dual-hypothesis assert (H-A/H-B live
                        // here). Scope-out with a watch active: the flag was
                        // captured from a model the deferred op's roll-forward
                        // may have outrun, and the true prediction epoch is
                        // only knowable after the resolution (stale-capture
                        // rule on [`resolve_keep_serving_watch`]). An
                        // engine-accepted conflicting merge mid-watch is
                        // still caught — as the resolution's no-composition
                        // `CrashContract` red: `apply_world` no-ops a
                        // predicted-conflict merge, so no hypothesis can
                        // render the merged state (the conflict is an
                        // absorbing element of the composition algebra).
                        if expected_conflict && keep_serving_watch.is_none() {
                            detectors::violation(
                                DET_MERGE_PREDICTION,
                                i,
                                format!(
                                    "engine ACCEPTED a merge the model predicts conflicts (op={wop:?})"
                                ),
                                "the engine's accept/conflict decision matches the three-way prediction",
                            );
                        }
                        // maintenance rewrites physical layout —
                        // prime invalidation moment; force a session check.
                        if matches!(
                            &wop,
                            WorldOp::Data {
                                op: Op::Optimize | Op::Cleanup | Op::EnsureIndices,
                                ..
                            }
                        ) {
                            force_session_check = true;
                        }
                        // A success on the watched handle ends the watch: the
                        // pending operation was resolved before this op ran,
                        // by the write entry's own heal (the issue-554
                        // contract holding). Premise scope-out: an op that
                        // bypasses the write entry (a view sync; an
                        // `EnsureIndices` not touching the pending branch)
                        // can succeed with the strand still pending — the
                        // resolution then cures the wedge; op-class filter is
                        // future work, the pinned scenarios never sample
                        // those mid-wedge. The succeeding op is NOT applied
                        // to the model first — it rides into the resolution
                        // as the known-applied interrupting op (the break
                        // shapes live on
                        // `dst_keep_serving_widened_arbitration_no_false_reds`);
                        // deferral contract on [`resolve_keep_serving_watch`].
                        if let Some(watch) = keep_serving_watch.take() {
                            let interrupt = WatchInterrupt {
                                wop: &wop,
                                applied: true,
                            };
                            let (new_db, _) = Box::pin(resolve_keep_serving_watch(
                                db,
                                storage.clone(),
                                root,
                                failing.as_deref(),
                                &mut world,
                                &mut reconcile_verdicts,
                                &mut known_issues,
                                &format!("{KEEP_SERVING_HEALED_PREFIX}op{i}"),
                                watch,
                                Some(&interrupt),
                            ))
                            .await;
                            db = new_db;
                        } else {
                            apply_world(&mut world, &wop);
                        }
                    }
                    Err(err) => {
                        // KEEP-SERVING (issue #554): with the budget armed,
                        // a `RecoveryRequired` refusal defers reconcile's
                        // reopen and keeps the SAME handle serving — the
                        // reopen runs Full recovery, the cure, so reopening
                        // on first contact structurally hides any wedge a
                        // long-lived server would sit in.
                        //
                        // The keep-serving `continue`s below deliberately
                        // skip the rest of this iteration: the damage-
                        // attribution window (a streak refusal is raised at
                        // the write entry BEFORE op execution, so no op
                        // reads crossed the ledger; a FRESH-watch op may
                        // have executed — the discovery-#3 arming shape —
                        // and its ledger crossing is deliberately dropped, a
                        // recorded telemetry-only gap: no judgment depends
                        // on the row), the `is_legal_rejection` catalog (the
                        // variant match on `RecoveryRequired` is a call-site
                        // catalog extension), and continuous verification (a
                        // mid-wedge world-match would judge a deliberately-
                        // held failure state, and `check_sessions`' fresh
                        // opens would heal the wedge under observation).
                        if sc.keep_serving_ops > 0
                            && let OmniError::RecoveryRequired { operation_id, .. } = &err
                            && let Some(mut watch) = keep_serving_watch
                                .take_if(|watch| &watch.operation_id == operation_id)
                        {
                            watch.streak += 1;
                            if watch.streak >= sc.keep_serving_ops {
                                detectors::violation(
                                    DET_LIVE_WRITE_AVAILABILITY,
                                    i,
                                    format!(
                                        "writes wedged on pending recovery operation {}: \
                                         {} consecutive RecoveryRequired refusals on the \
                                         live handle (first at op{}), reopen deferred",
                                        watch.operation_id, watch.streak, watch.first_op
                                    ),
                                    Oracle::LiveWriteAvailability.doc(),
                                );
                            }
                            known_issues.push(keep_serving_defer_row(i, &watch.operation_id));
                            keep_serving_watch = Some(watch);
                            legal_rejections += 1;
                            continue;
                        }
                        // A refusal against the SAME pending strand can
                        // arrive as the clean-recovery-state spelling (the
                        // `manifest_conflict` "requires a clean recovery
                        // state" text — keyed on the TEXT, not an op set, so
                        // any future emitter rides this branch too), not
                        // typed `RecoveryRequired` — the engine's second
                        // spelling of the wedge. Ending the watch on it would
                        // reopen and CURE the wedge under observation — see
                        // the arm-intro comment above. It continues the watch
                        // as a defer row but does NOT count toward the
                        // budget: the oracle's contract counts refusals
                        // naming one operation id, and this spelling names
                        // none. Supersedes the `reopen-heals-barrier@` tag
                        // mid-watch — the defer row encodes the encounter.
                        if keep_serving_watch.is_some()
                            && !matches!(&err, OmniError::RecoveryRequired { .. })
                            && format!("{err:?}").contains(CLEAN_RECOVERY_BARRIER_TEXT)
                        {
                            known_issues.push(keep_serving_defer_row(i, "recovery-barrier"));
                            legal_rejections += 1;
                            continue;
                        }
                        // Any OTHER failure while a watch is active ends the
                        // wedge experiment before this failure's own handling
                        // — deferral contract on [`resolve_keep_serving_watch`].
                        // A failure class that can leave durable effects
                        // (fault-marked, ack-lost, damage-attributed) rides
                        // into the resolution as the UNCERTAIN interrupting
                        // op and is judged there. A DIFFERENT-id
                        // `RecoveryRequired` is in that class too: a same-id
                        // refusal never reaches here (the streak branch), so
                        // a watch-ending `RecoveryRequired` names a FRESH
                        // strand this op armed by executing and failing
                        // mid-write. Only a plain legal rejection (no marker,
                        // no damage, no recovery arm) provably left nothing
                        // and resolves with no interrupt.
                        let mut interrupt_judged: Option<(ReconcileOutcome, &'static str)> = None;
                        let mut watch_resolved = false;
                        // One damage snapshot for the WHOLE failure handling:
                        // persisted-damage consumption counts through
                        // suspension by design, so a post-resolution read of
                        // the ledger would blame this op for the resolution's
                        // own reads. Captured once, pre-resolution, shared by
                        // the uncertainty classifier and the attribution
                        // below.
                        let damaged_now = failing
                            .as_ref()
                            .map(|f| f.damage_events())
                            .unwrap_or(0)
                            > damage_before;
                        if let Some(watch) = keep_serving_watch.take() {
                            let err_text = format!("{err:?}");
                            let uncertain = matches!(&err, OmniError::RecoveryRequired { .. })
                                || err_text.contains(FAULT_MARKER)
                                || err_text.contains(ACK_LOSS_MARKER)
                                || damaged_now;
                            let interrupt = WatchInterrupt {
                                wop: &wop,
                                applied: false,
                            };
                            let (new_db, ruling) = Box::pin(resolve_keep_serving_watch(
                                db,
                                storage.clone(),
                                root,
                                failing.as_deref(),
                                &mut world,
                                &mut reconcile_verdicts,
                                &mut known_issues,
                                &format!("{KEEP_SERVING_INTERRUPTED_PREFIX}op{i}"),
                                watch,
                                uncertain.then_some(&interrupt),
                            ))
                            .await;
                            db = new_db;
                            interrupt_judged = ruling;
                            watch_resolved = true;
                        }
                        // Stale-capture rule on
                        // [`resolve_keep_serving_watch`]: re-derive the
                        // merge prediction from the post-ruling model for
                        // every judgment of THIS op below.
                        let expected_conflict = if watch_resolved {
                            expects_merge_conflict(&world, &wop)
                        } else {
                            expected_conflict
                        };
                        // Fresh watch: this failure names a pending recovery
                        // operation nothing is watching yet — defer its
                        // reconcile and start counting. The budget check runs
                        // here too, so `keep_serving_ops: 1` fires on the
                        // FIRST refusal as the field doc promises. Skipped
                        // when an interrupt-resolution just judged this op:
                        // its strand was healed by that resolution's reopen,
                        // so no pending operation is left to watch, and a
                        // fresh watch would re-judge a judged op — the
                        // exactly-once contract on [`WatchRuling::e_outcome`].
                        if interrupt_judged.is_none()
                            && sc.keep_serving_ops > 0
                            && let OmniError::RecoveryRequired { operation_id, .. } = &err
                        {
                            let watch = KeepServingWatch {
                                operation_id: operation_id.clone(),
                                first_op: i,
                                streak: 1,
                                deferred_wop: wop.clone(),
                            };
                            if watch.streak >= sc.keep_serving_ops {
                                detectors::violation(
                                    DET_LIVE_WRITE_AVAILABILITY,
                                    i,
                                    format!(
                                        "writes wedged on pending recovery operation {}: \
                                         refused on first contact with a keep-serving budget \
                                         of {}, reopen deferred",
                                        watch.operation_id, sc.keep_serving_ops
                                    ),
                                    Oracle::LiveWriteAvailability.doc(),
                                );
                            }
                            known_issues.push(keep_serving_defer_row(i, operation_id));
                            keep_serving_watch = Some(watch);
                            legal_rejections += 1;
                            continue;
                        }
                        // attributed detection: this op's reads
                        // crossed the damage ledger, so its (engine-born,
                        // unmarked) failure is the detection half of the
                        // detected-or-harmless contract — legal, recorded
                        // for typed-vs-raw triage. Detection quality is
                        // first-contact evidence, not a pass/fail axis yet.
                        if damaged_now {
                            // Root-normalized: the report must stay
                            // root-independent so same-seed universes on
                            // different roots replay-compare equal.
                            let text = format!("{err:?}").replace(root, "<root>");
                            let snippet: String = text.chars().take(240).collect();
                            corruption_detections.push(format!("op{i} {wop:?}: {snippet}"));
                        }
                        // Call-site catalog extension by VARIANT: a
                        // watch-ending `RecoveryRequired` the resolution just
                        // judged is legal per se — relying on the engine
                        // embedding its cause's marker text into the error
                        // would couple legality to message formatting. With
                        // `keep_serving_ops: 0` a typed `RecoveryRequired`
                        // reaching this check reds — a correct tripwire: no
                        // v1-shaped universe can produce one here (every
                        // failure reconciles in its own iteration).
                        let judged_recovery_refusal = interrupt_judged.is_some()
                            && matches!(&err, OmniError::RecoveryRequired { .. });
                        if !(damaged_now
                            || judged_recovery_refusal
                            || is_legal_rejection(&err, &world, &wop, expected_conflict))
                        {
                            detectors::violation(
                                DET_LEGAL_CLAIM,
                                i,
                                format!("illegal op failure (op={wop:?}): {err:?}"),
                                "every op failure matches the definite-error catalog",
                            );
                        }
                        legal_rejections += 1;
                        // Tag known-defect encounters
                        // with their tracking references.
                        if is_recovery_barrier_rejection(&wop, &err) {
                            known_issues.push(format!("reopen-heals-barrier@op{i}"));
                        }
                        // CLIENT RETRY after ack-loss — semantics on
                        // `FaultPlan::client_retry`. Reconcile below still
                        // arbitrates the settled world either way. Skipped
                        // when the watch resolution already judged this op:
                        // its reopen superseded the state the op failed
                        // under, and a retry would execute against a world
                        // the ruling already installed in the model
                        // (keep-serving scenarios do not set client_retry;
                        // the combination is scoped out, not exercised).
                        if interrupt_judged.is_none()
                            && format!("{err:?}").contains(ACK_LOSS_MARKER)
                            && sc
                                .faults
                                .as_ref()
                                .map(|p| p.client_retry)
                                .unwrap_or(false)
                        {
                            client_retries += 1;
                            // Boxed: a SECOND exec_world_op future in this
                            // frame (2 MiB test stack; overflowed inline on
                            // first run).
                            if let Err(retry_err) = Box::pin(exec_world_op(&mut db, &wop)).await {
                                // FIRST-CONTACT VERDICT (2026-08-12): a
                                // retry after an ack-lost mutation meets the
                                // RECOVERY BARRIER — typed `RecoveryRequired`
                                // naming the remedy ("reopen read-write
                                // before retrying"). By design; the reconcile
                                // below performs that reopen. One lost ack
                                // costs the handle write capability until
                                // reopen.
                                let retry_text = format!("{retry_err:?}");
                                if retry_text.contains("RecoveryRequired") {
                                    known_issues
                                        .push(format!("recovery-barrier-on-retry@op{i}"));
                                }
                                if !(retry_text.contains("RecoveryRequired")
                                    || is_legal_rejection(
                                        &retry_err,
                                        &world,
                                        &wop,
                                        expected_conflict,
                                    ))
                                {
                                    detectors::violation(
                                        DET_ACK_LOSS,
                                        i,
                                        format!(
                                            "illegal RETRY failure after ack-loss (op={wop:?}): {retry_err:?}"
                                        ),
                                        "a retry against the client's own durable success fails only in cataloged shapes",
                                    );
                                }
                            }
                        }
                        // ack-loss failures MUST reconcile —
                        // their effects are usually durable, and only the
                        // two-picture arbitration can decide Applied vs
                        // NotApplied (silently assuming "failed ⇒
                        // invisible" was v0's original bug).
                        // Row context is honest provenance: a judged
                        // interrupt records as `watch-interrupt@` — its
                        // failure need not be an injected fault (a fresh
                        // different-id `RecoveryRequired` is engine-born).
                        let fault_verdict: Option<(ReconcileOutcome, &'static str, &'static str)> =
                            if let Some((outcome, channel)) = interrupt_judged {
                                // Exactly-once contract on
                                // [`WatchRuling::e_outcome`] — row only.
                                Some((outcome, channel, "watch-interrupt"))
                            } else if format!("{err:?}").contains(FAULT_MARKER)
                                || format!("{err:?}").contains(ACK_LOSS_MARKER)
                                // corruption-attributed failures (and
                                // marked latent sector errors, which advance the
                                // same ledger) MUST reconcile — the op may have
                                // durably written before its poisoned read, and
                                // only the two-picture arbitration can rule
                                // Applied vs NotApplied on a garbage-fed op.
                                || damaged_now
                                || is_recovery_barrier_rejection(&wop, &err)
                            {
                                // The engine arms recovery and bars
                                // writes until reopen — behave like a real client.
                                // Reconcile runs fault-suspended (reopen-under-storm
                                // resilience proven in the pre-suspension run).
                                if let Some(f) = &failing {
                                    f.suspend();
                                }
                                let (new_db, outcome, channel) = Box::pin(reconcile_after_failure(
                                    db,
                                    storage.clone(),
                                    root,
                                    &wop,
                                    &world,
                                    "injected-fault failure",
                                    i,
                                    None,
                                ))
                                .await;
                                db = new_db;
                                if let Some(f) = &failing {
                                    f.resume();
                                }
                                outcome.apply(&mut world, &wop);
                                Some((outcome, channel, "fault"))
                            } else {
                                None
                            };
                        if let Some((outcome, channel, context)) = fault_verdict {
                            reconcile_verdicts.push((
                                format!("{context}@op{i}"),
                                format!("{outcome:?}"),
                                channel.to_string(),
                            ));
                        }
                    }
                }
            }

            // CONTINUOUS VERIFICATION (slatedb's verify_* idea, exact-model
            // strength): every third op the WHOLE world — branch list plus
            // every branch's persons and edges — must equal the model NOW,
            // and one live query must agree on membership.
            if i % 3 == 2 || force_session_check {
                if let Some(f) = &failing {
                    f.suspend();
                }
                crate::cost::set_label("_verify");
                if i % 3 == 2 && !sc.ablate_verify {
                    let world_match_here = match (
                        sc.world_match_only_at,
                        sc.world_match_from,
                        sc.world_match_until,
                    ) {
                        (Some(k), _, _) => i == k,
                        (None, Some(from), _) => i >= from,
                        (None, None, Some(until)) => i < until,
                        (None, None, None) => !sc.ablate_world_match,
                    };
                    if world_match_here {
                        detectors::tagged(
                            DET_WORLD,
                            i,
                            assert_world_matches(&db, &world, &format!("mid-run (op {i})")),
                        )
                        .await;
                    }
                    // (membership query below stays live under
                    // ablate_world_match — the split's whole point)
                    if let Some(name) = world.main.persons.keys().next().cloned() {
                        let qr = query_main(
                            &db,
                            MUTATION_QUERIES,
                            "get_person",
                            &mixed_params(&[("$name", &name)], &[]),
                        )
                        .await
                        .expect("mid-run read query");
                        if qr.num_rows() != 1 {
                            detectors::violation(
                                DET_MEMBERSHIP,
                                i,
                                format!(
                                    "mid-run get_person for '{name}' returned {} rows",
                                    qr.num_rows()
                                ),
                                "exactly 1 row (the model holds the person)",
                            );
                        }
                    }
                    verified += 1;
                }
                // both Expand modes + the bound arm, on main,
                // in whatever state the universe is in right now.
                if !sc.ablate_mode_arms {
                    detectors::tagged(
                        DET_TRAVERSAL,
                        i,
                        Box::pin(assert_traversal_modes_agree(
                            &db,
                            &world,
                            std::slice::from_ref(&MAIN_BRANCH),
                            &format!("mode check (op {i})"),
                        )),
                    )
                    .await;
                }
                // the session oracle — on the verification
                // cadence AND immediately after maintenance ops; every 3rd
                // session check runs the scheduled catch-up (the cure as an
                // invariant).
                if !sc.ablate_sessions {
                    session_checks += 1;
                    detectors::tagged(
                        DET_SESSION,
                        i,
                        Box::pin(check_sessions(
                            &db,
                            &bystander,
                            root,
                            &storage,
                            &world,
                            &history,
                            &mut bystander_last,
                            &mut bystander_trail,
                            session_checks.is_multiple_of(3),
                            &format!("session check (op {i})"),
                        )),
                    )
                    .await;
                }
                force_session_check = false;
                if let Some(f) = &failing {
                    f.resume();
                }
            }
        }

        // A watch outliving the op loop (the wedge stayed under the budget)
        // resolves before the closing oracles, so the final audit never
        // inherits an unjudged pending operation — deferral contract on
        // [`resolve_keep_serving_watch`]. No interrupt: no op is in flight
        // at loop end. Closing traffic: its reopen and reads bill to
        // `_close`, never the last op's row.
        crate::cost::set_label("_close");
        if let Some(watch) = keep_serving_watch.take() {
            let (new_db, _) = Box::pin(resolve_keep_serving_watch(
                db,
                storage.clone(),
                root,
                failing.as_deref(),
                &mut world,
                &mut reconcile_verdicts,
                &mut known_issues,
                KEEP_SERVING_EXPIRED_PREFIX,
                watch,
                None,
            ))
            .await;
            db = new_db;
        }

        // Closing oracle phase runs on clean storage — under its own cost
        // label, so the loop's final op row never absorbs the closing
        // traffic (the 1-op floor cells made that pollution visible;
        // measured: ~36 l.put of closing ensure_indices landing on the
        // last op's row).
        crate::cost::set_label("_close");
        if let Some(f) = &failing {
            f.suspend();
        }
        // LIVENESS lever: convergence must COMPLETE, not merely be correct.
        // A hung future trips this bound; a consistent-but-deadlocked system
        // is the failure mode all the state oracles are blind to.
        //
        // BOUND ON THE REAL CLOCK (fix 2026-08-10, the virtual-timeout root cause):
        // under start_paused, tokio AUTO-ADVANCES virtual time whenever the
        // runtime idles — and since de-vendoring, index compute dispatches to
        // the foreign lance-cpu OS thread again, so a virtual-time timeout
        // elapses instantly while real cross-thread work is still running
        // (regression pins: dst_liveness_oracle_survives_cross_thread_work +
        // the zero-op control). resume() switches to the real clock for the
        // guarded await (deadlock-only detector; 120s wall on a toy fixture),
        // pause() restores virtual time for the rest of the universe.
        tokio::time::resume();
        let lively = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            Box::pin(db.ensure_indices()),
        )
        .await;
        tokio::time::pause();
        let lively = match lively {
            Ok(inner) => inner,
            Err(_elapsed) => detectors::violation(
                DET_LIVENESS,
                sc.ops,
                "LIVENESS: ensure_indices did not converge within bound".to_string(),
                "convergence completes within the 120 s real-clock bound",
            ),
        };
        // FIRST-CONTACT FINDING of the corruption axis
        // (2026-08-13, seed 97): a FOREIGN-NAMED sidecar permanently blocks
        // maintenance — the recovery BARRIER parses the file's CONTENT
        // (pending Mutation, op id) via directory listing, but the HEALER
        // deletes `sidecar_uri(root, operation_id)` — the canonical path
        // reconstructed from the op id (recovery.rs:7995), NOT the listed
        // file's actual path — so the dstm- file is re-"healed" every
        // reopen yet never removed, and the typed RecoveryRequired remedy
        // ("reopen") provably does not clear it. Named carve-out: tolerate
        // + record ONLY when the barrier names a foreign sidecar's op and
        // foreign damage was injected; every other failure still panics.
        if let Err(err) = lively {
            let text = format!("{err:?}");
            let foreign_injected = failing
                .as_ref()
                .map(|f| {
                    f.persisted_damage_snapshot()
                        .values()
                        .any(|v| *v == "misdirect-target")
                })
                .unwrap_or(false);
            assert!(
                foreign_injected && text.contains("RecoveryRequired"),
                "ensure_indices in-universe: {err:?}"
            );
            FOREIGN_SIDECAR_ROWS.lock().unwrap().push(format!(
                "s11b-foreign-sidecar-blocks-maintenance@final-audit: {}",
                text.replace(root, "<root>").chars().take(160).collect::<String>()
            ));
        }
        // closing capture — the loop's final op plus the closing
        // ensure_indices' own commit, if it made one.
        Box::pin(capture_history(&db, &world.main, &mut history)).await;

        // CORRUPTION AXIS (persisted tier) — pre-reopen residue evidence: injected lost
        // deletes / misdirected writes park sidecar residue with NO failure
        // routing through reconcile. Record what is parked and by which
        // verb (attributed against the persisted ledger) BEFORE the final
        // reopen heals; the post-reopen assert below then holds the
        // reopen-heals contract over injected residue too —
        // recorded, never excused.
        let mut attributed_residue: Vec<String> = Vec::new();
        if let Some(f) = &failing {
            let ledger = f.persisted_damage_snapshot();
            if !ledger.is_empty() {
                for uri in recovery_residue(&storage, root).await {
                    if let Some(verb) = ledger.get(&uri) {
                        // Root-normalized: the report replay-compares
                        // across roots.
                        attributed_residue.push(format!("{verb} {}", uri.replace(root, "<root>")));
                    }
                }
            }
        }

        // Durability + full oracle through a FRESH read-write handle.
        crate::cost::set_label("_audit");
        drop(db);
        let db = Omnigraph::open_with_storage(root, storage.clone())
            .await
            .expect("final reopen");
        detectors::tagged(
            DET_WORLD,
            sc.ops,
            assert_world_matches(&db, &world, "final reopen"),
        )
        .await;
        // Recovery obligation at QUIESCE (audit follow-up): reconcile
        // asserts residue-emptiness only when a failure routed through it —
        // a legal rejection without reconcile could park residue to the end
        // of the universe unexamined. No universe ends owing recovery work.
        // Persisted tier: injected residue must ALSO heal on this reopen — the
        // message names the injected verb when the survivor is attributed.
        let final_residue = recovery_residue(&storage, root).await;
        let final_residue = partition_residue(final_residue, root, "final audit");
        if !final_residue.is_empty() {
            let ledger = failing
                .as_ref()
                .map(|f| f.persisted_damage_snapshot())
                .unwrap_or_default();
            let tagged: Vec<String> = final_residue
                .iter()
                .map(|uri| match ledger.get(uri) {
                    Some(verb) => format!("{uri} (injected: {verb} — reopen did NOT heal it)"),
                    None => uri.clone(),
                })
                .collect();
            detectors::violation(
                DET_RESIDUE,
                sc.ops,
                format!("final audit: sidecar residue at quiesce: {tagged:?}"),
                "no universe ends owing recovery work (__recovery/ empty at quiesce)",
            );
        }
        // the physical channel (row dump via export_jsonl) must agree too.
        detectors::tagged(
            DET_PHYSICAL,
            sc.ops,
            Box::pin(assert_physical_matches(&db, &world, "final reopen")),
        )
        .await;
        // both Expand modes + the bound arm, every branch.
        detectors::tagged(
            DET_TRAVERSAL,
            sc.ops,
            Box::pin(assert_traversal_modes_agree(
                &db,
                &world,
                &world.branch_names(),
                "final reopen",
            )),
        )
        .await;
        // the history channel — the PAST must still be intact,
        // not just the present; every recorded commit at or after the
        // retention horizon re-read through the time-travel surface.
        detectors::tagged(
            DET_HISTORY,
            sc.ops,
            Box::pin(assert_history_matches(
                &db,
                &history[history_verified_from..],
                "final reopen",
            )),
        )
        .await;
        // convergence at quiesce (VOPR pattern): no session may
        // end the universe wedged; catch-up forced so the bystander must
        // land on the present.
        detectors::tagged(
            DET_SESSION,
            sc.ops,
            Box::pin(check_sessions(
                &db,
                &bystander,
                root,
                &storage,
                &world,
                &history,
                &mut bystander_last,
                &mut bystander_trail,
                true,
                "final quiesce",
            )),
        )
        .await;
        drop(bystander);

        // Query-channel variant: the READ-ONLY open path must agree (main;
        // branch reads through the read-only path are a candidate widening).
        let ro = Omnigraph::open_read_only_with_storage(root, storage)
            .await
            .expect("read-only reopen");
        detectors::tagged(
            DET_RO_AUDIT,
            sc.ops,
            assert_matches_model(&ro, &world.main, "read-only view"),
        )
        .await;
        drop(ro);

        // Full-read-path digest: compiler→DataFusion read, row order included.
        let query_digest = query_main(&db, MUTATION_QUERIES, "all_persons", &Default::default())
            .await
            .expect("query-digest read must succeed on a quiesced universe")
            .to_rust_json()
            .to_string();

        // OCC invariant, per branch (cross-branch id reuse is legal — shared
        // fork lineage — so uniqueness is asserted within each history).
        let mut ids: Vec<String> = Vec::new();
        for name in world.branch_names() {
            let commits = db
                .list_commits(Some(name.as_str()))
                .await
                .expect("list commits");
            let branch_ids: Vec<String> =
                commits.iter().map(|c| c.graph_commit_id.clone()).collect();
            let unique: BTreeSet<&String> = branch_ids.iter().collect();
            if unique.len() != branch_ids.len() {
                detectors::violation(
                    DET_OCC,
                    sc.ops,
                    format!(
                        "OCC invariant: duplicate graph_commit_id on '{name}' \
                         ({} ids, {} unique)",
                        branch_ids.len(),
                        unique.len()
                    ),
                    "graph_commit_ids are unique within one branch's history",
                );
            }
            if name == "main" {
                ids = branch_ids;
            }
        }

        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
        crate::lance_faults::set_active(None);
        crate::lance_faults::set_kill(None);
        // WRITE CENSUS bottom listings: with weather and kill cleared,
        // read each realm's store from below (the surface a bypass writer
        // cannot avoid) so the census test can reconcile recorded writes
        // against ground truth.
        if crate::write_census::recording() {
            let adapter_keys = concrete_adapter
                .dst_list_all_keys()
                .await
                .unwrap_or_default();
            let lance_keys = crate::lance_faults::list_realm_keys(root).await;
            crate::write_census::set_final_keys(adapter_keys, lance_keys);
        }
        let lance_realm_injected = lance_faults_state
            .as_ref()
            .map(|s| s.injected())
            .unwrap_or(0);
        let writes_observed = kill_state
            .as_ref()
            .map(|s| s.writes_observed())
            .unwrap_or(0);
        let crash_state_hit = kill_state.as_ref().map(|s| s.hit()).unwrap_or(false);
        let acks_lost = failing.as_ref().map(|f| f.acks_lost()).unwrap_or(0);
        let reads_corrupted = failing.as_ref().map(|f| f.reads_corrupted()).unwrap_or(0);
        let reads_truncated = failing.as_ref().map(|f| f.reads_truncated()).unwrap_or(0);
        let latent_errors = failing.as_ref().map(|f| f.latent_errors()).unwrap_or(0);
        let writes_corrupted = failing.as_ref().map(|f| f.writes_corrupted()).unwrap_or(0);
        let writes_lost = failing.as_ref().map(|f| f.writes_lost()).unwrap_or(0);
        let writes_misdirected = failing.as_ref().map(|f| f.writes_misdirected()).unwrap_or(0);
        let persisted_consumed = failing.as_ref().map(|f| f.persisted_consumed()).unwrap_or(0);
        let stale_reads_served = failing.as_ref().map(|f| f.stale_reads_count()).unwrap_or(0);
        let stale_lists_served = failing.as_ref().map(|f| f.stale_lists_count()).unwrap_or(0);
        // Persisted tier: drain the foreign-sidecar carve-out rows into the
        // known-issues column (insertion order — deterministic).
        known_issues.extend(FOREIGN_SIDECAR_ROWS.lock().unwrap().drain(..));
        UniverseReport {
            end_state: world.main.person_rows(),
            edges: world.main.edge_pairs(),
            world: world.render(),
            commit_ids: ids,
            query_digest,
            crashes,
            crossed: crossed_flag.load(std::sync::atomic::Ordering::SeqCst),
            ghost_edges: world.main.ghosts.iter().cloned().collect(),
            history_commits: history[history_verified_from..]
                .iter()
                .map(|(id, _)| id.clone())
                .collect(),
            bystander_trail,
            verified,
            legal_rejections,
            lance_realm_injected,
            writes_observed,
            crash_state_hit,
            acks_lost,
            client_retries,
            maintenance_reruns,
            reads_corrupted,
            reads_truncated,
            latent_errors,
            corruption_detections,
            writes_corrupted,
            writes_lost,
            writes_misdirected,
            persisted_consumed,
            attributed_residue,
            reconcile_verdicts,
            known_issues,
            stale_reads_served,
            stale_lists_served,
        }
    }))
            })
            .expect("spawn universe thread")
            .join()
    });
    // Exit-side leak clear, BOTH outcomes — rationale on
    // [`clear_process_slots`].
    clear_process_slots();
    result
}

// Pure-classifier honesty (unit level): the mutation
// functions must provably change text, respect char boundaries, and be
// deterministic in their inputs. The universe-level per-verb non-vacuity
// pins live in `tests/scenarios.rs` (the corruption pins).
#[cfg(test)]
mod corruption_verb_tests {
    use super::{bit_rot_text, truncate_text};

    #[test]
    fn bit_rot_always_changes_and_respects_char_boundaries() {
        // Unicode content (the hostile-key alphabet's class): mutation must
        // land on a char, never mid-codepoint, at every position.
        for text in ["{\"k\":1}", "\u{00e9}\u{00e8}\u{00ea}", "x", "##", "12345"] {
            let count = text.chars().count() as u64;
            for pos in 0..count {
                let rotted = bit_rot_text(text, pos).expect("non-empty");
                assert_ne!(rotted, text, "pos {pos} of {text:?} must change");
                assert_eq!(rotted.chars().count(), text.chars().count());
            }
        }
        assert_eq!(bit_rot_text("", 0), None);
    }

    #[test]
    fn truncate_is_a_strict_prefix() {
        for text in ["{\"k\":1}", "\u{00e9}\u{00e8}\u{00ea}", "x"] {
            let count = text.chars().count() as u64;
            for pos in 0..count {
                let cut = truncate_text(text, pos).expect("non-empty");
                assert!(cut.chars().count() < text.chars().count());
                assert!(text.starts_with(&cut));
            }
        }
        assert_eq!(truncate_text("", 3), None);
    }

    #[test]
    fn mutations_are_deterministic() {
        assert_eq!(bit_rot_text("abcdef", 3), bit_rot_text("abcdef", 3));
        assert_eq!(truncate_text("abcdef", 3), truncate_text("abcdef", 3));
    }

    #[test]
    fn misdirect_keeps_directory_and_extension() {
        assert_eq!(
            super::misdirect_uri("shared-memory://r/__recovery/op123.json"),
            "shared-memory://r/__recovery/dstm-op123.json"
        );
        assert_eq!(super::misdirect_uri("bare"), "dstm-bare");
    }
}
