//! The universe runner.
//!
//! One universe = one seeded, single-threaded, in-memory world driving a real
//! `Omnigraph` through its production write path, checked by an edge-aware
//! differential model + referential-integrity oracle, CONTINUOUS verification
//! (exact-model equality every third op), versioned values, seeded storage
//! faults at the StorageAdapter seam (latency sleeps VIRTUAL time), a
//! read-only third audit view, and seed logging for CI reproducibility.

use std::collections::{BTreeMap, BTreeSet};
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
    /// (usually durable) success — upserts must converge, a re-merge is an
    /// empty-delta merge (the version-collision shape). The retry's error surface is
    /// held to `is_legal_rejection` STRICTLY: any novel
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
    writes: std::sync::atomic::AtomicUsize,
    dead: std::sync::atomic::AtomicBool,
    enabled: std::sync::atomic::AtomicBool,
    suspended: std::sync::atomic::AtomicBool,
    hit: std::sync::atomic::AtomicBool,
    killed_label: Mutex<Option<String>>,
}

impl KillState {
    pub(crate) fn new(k: usize) -> Arc<Self> {
        Arc::new(Self {
            k,
            writes: std::sync::atomic::AtomicUsize::new(0),
            dead: std::sync::atomic::AtomicBool::new(false),
            enabled: std::sync::atomic::AtomicBool::new(false),
            suspended: std::sync::atomic::AtomicBool::new(false),
            hit: std::sync::atomic::AtomicBool::new(false),
            killed_label: Mutex::new(None),
        })
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

    /// Process restart: storage answers again, and the enumerator is done
    /// for this universe (one death per universe, like crash windows).
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
        if n == self.k {
            self.dead.store(true, std::sync::atomic::Ordering::SeqCst);
            self.hit.store(true, std::sync::atomic::Ordering::SeqCst);
            *self.killed_label.lock().unwrap() = Some(format!("{op} {uri}"));
            return Err(format!("{KILL_MARKER}: write #{n} {op} {uri}"));
        }
        Ok(())
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
    /// model-predicted merge conflict).
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
    /// (context, verdict, channel) where context names what died
    /// (`crash:<window>@op<i>`, `crash-state:write#k@op<i>`,
    /// `fault@op<i>`), verdict is `Applied` / `ForkOnly` / `NotApplied`,
    /// and channel is the observation surface the ruling rested on
    /// ("query", or "query+physical" when the ghost tie-break consulted
    /// the physical channel). The per-death RESULT the ledger records for
    /// hits. Deterministic and replay-compared: an arbitration that flips
    /// between same-seed runs is itself a caught bug.
    pub reconcile_verdicts: Vec<(String, String, String)>,
    /// Known-defect encounters this universe had —
    /// carve-outs and by-design behaviors firing during workload ops, each
    /// tagged with its tracking reference (e.g. `#473-
    /// no-op-republish@op7`, `reopen-heals-barrier@op12`,
    /// `recovery-barrier-on-retry@op9`). The "did this run meet any
    /// known issue" column.
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
        if branch == "main" {
            &self.main
        } else {
            &self.branches[branch].state
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
        if std::env::var("DST_PREDICT_LOG").is_ok() {
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
    /// it joins the logically-invisible set the model ignores. Currently
    /// QUARANTINED from the sampler by the schema-add poisoned-read finding (see the roll-12
    /// note) — dead_code allowed until the fix re-enables it.
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
        // Roll 12 is the SchemaAddProperty slot, quarantined by ENGINE
        // KNOWN ENGINE FINDING (2026-08-11, first sample): apply_schema after
        // ANY mutation breaks subsequent traversals on the live handle
        // (Arrow "same length" mismatch; live-handle-only, refresh() does NOT
        // heal, only reopen; pinned in `dst_schema_add_property_after_mutation_breaks_traversal`). Until the engine
        // fix lands the slot doubles the load frequency instead (1/16 was
        // thin: whole 24-op streams sampled zero loads) — then restore
        // `12 => { *schema_extras += 1; SchemaAddProperty{count} }` and
        // re-add schema_apply/schema_reload to `workload_can_reach`.
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
            // schema_apply/schema_reload: machinery ready (SchemaAddProperty
            // + window_matches), but the op is quarantined from the sampler
            // by the schema-add finding — re-add the two families here with the fix.
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
        WorldOp::BranchMerge { source } => {
            let slot = &world.branches[source.as_str()];
            predict_merge(&slot.base, &slot.state, &world.main).is_none()
        }
        _ => false,
    }
}

fn is_merge_conflict_err(err: &OmniError) -> bool {
    format!("{err:?}").contains("MergeConflict")
}

/// THE VERSION-COLLISION FINDING (2026-08-10): merging a branch whose table
/// version count advanced past the fork point can fail PERMANENTLY with an
/// unclassified `Lance("Concurrent modification: table version N already
/// exists ...")` — the manifest publisher keys table versions by (identity,
/// version) across lineages (publisher.rs:448/:462). Deterministic repro:
/// `dst_merge_version_collision_diverged_edge_table`. Until the
/// engine fix lands the harness classifies THIS exact failure as a known
/// legal rejection so the hunt keeps moving; drop this carve-out with the
/// pin test.
fn is_known_version_collision(wop: &WorldOp, err: &OmniError) -> bool {
    matches!(wop, WorldOp::BranchMerge { .. })
        && format!("{err:?}").contains("already exists for identity")
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
fn is_recovery_barrier_rejection(wop: &WorldOp, err: &OmniError) -> bool {
    matches!(
        wop,
        WorldOp::Data {
            op: Op::Optimize | Op::Cleanup | Op::SchemaAddProperty { .. },
            ..
        }
    ) && format!("{err:?}").contains("requires a clean recovery state")
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
            self.staleness_record(&target, Some(stored.clone()), None);
        }
        self.lose_ack("write_text", uri, out).await
    }
    async fn write_text_if_absent(&self, uri: &str, contents: &str) -> OmniResult<bool> {
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
            self.staleness_record(&target, Some(stored.clone()), None);
        }
        self.lose_ack("write_text_if_absent", uri, out).await
    }
    async fn exists(&self, uri: &str) -> OmniResult<bool> {
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
            self.staleness_record(from_uri, None, None);
            self.staleness_record(to_uri, moved, None);
        }
        self.lose_ack("rename_text", from_uri, out).await
    }
    async fn delete(&self, uri: &str) -> OmniResult<()> {
        if let WriteFate::Lost = self.write_fault("delete", uri, true).await? {
            return Ok(());
        }
        self.staleness_base(uri).await;
        let out = self.inner.delete(uri).await;
        if out.is_ok() {
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
            self.staleness_record(uri, Some(stored.clone()), Some(new_token.clone()));
        }
        self.lose_ack("write_text_if_match", uri, out).await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> OmniResult<()> {
        if let WriteFate::Lost = self.write_fault("delete_prefix", prefix_uri, true).await? {
            return Ok(());
        }
        let out = self.inner.delete_prefix(prefix_uri).await;
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
        let expected: Vec<(String, String)> = m.edges.union(&m.ghosts).cloned().collect();
        assert_eq!(
            knows, expected,
            "{where_}: EXPORT Knows diverged from model (logical ∪ ghosts) on '{branch}'"
        );
    }
}

fn is_legal_rejection(
    err: &OmniError,
    world: &WorldModel,
    wop: &WorldOp,
    expected_conflict: bool,
) -> bool {
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
    if is_known_version_collision(wop, err) {
        return true;
    }
    if is_recovery_barrier_rejection(wop, err) {
        return true;
    }
    // RI hypothesis: deleting a person with live edges may be refused.
    if let WorldOp::Data {
        branch,
        op: Op::DeletePerson { name },
    } = wop
        && world.state_of(branch).has_edges_touching(name)
        && (text.contains("referential") || text.contains("edge"))
    {
        return true;
    }
    false
}

/// How a failed op's world state settled after reconcile + recovery reopen.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// After ANY failed op (crash window or injected fault): assert atomicity,
/// reopen over the same storage (= the recovery sweep; a
/// fault-killed mutation arms a recovery sidecar and the engine BLOCKS
/// further writes behind a recovery barrier until a read-write reopen), then
/// assert recovery monotonicity and report how the op settled. World-level:
/// the hypotheses (op invisible
/// XOR op applied, plus the fork-survives third state for `LoadFork`) cover
/// branch existence and every branch's state, so torn branch
/// creates/deletes/merges violate atomicity exactly like torn mutations.
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
    let with = {
        let mut w = world.clone();
        apply_world(&mut w, wop);
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

    // DOUBLE-FAULT lever: kill the FIRST recovery sweep mid-pass, then
    // prove a second (clean) reopen still converges to a legal state.
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

    // The recovery sweep can ITSELF hit injected faults (it writes sidecars) — a real
    // client retries. Bounded and seeded, so still deterministic.
    let mut reopen_attempts = 0u32;
    let db = loop {
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
                    reopen_attempts < 16,
                    "{label}: recovery never survived the fault storm"
                );
            }
        }
    };
    let after = observe_world(&db).await;
    if !legal(&after) {
        detectors::violation(
            DET_CRASH_CONTRACT,
            at_op,
            format!("{label}: recovery produced an illegal state (op={wop:?}); after={after:?}"),
            "post-recovery world renders as base, applied, or fork-only",
        );
    }
    // RECOVERY-OBLIGATION ORACLE (2026-08-12, from the seeded-recovery-no-op
    // honesty experiment): the state hypotheses alone CANNOT distinguish a
    // correct rollback from a recovery that silently did nothing — "not
    // applied" is always legal, and the barrier carve-out excuses subsequent
    // rejections. With heal_pending_sidecars_roll_forward stubbed to a
    // no-op, 25 of 26 tests stayed green; only the discovery-5 pin (which
    // asserts the healing SIDE-EFFECT) went red. So assert recovery's
    // obligation, not just state legality: a successful read-write reopen
    // leaves no sidecar residue (the reopen-heals contract).
    // Runs fault-suspended (callers suspend around reconcile), so this read
    // is clean.
    let residue = recovery_residue(&storage, root).await;
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
    let mut outcome = if after == as_with {
        ReconcileOutcome::Applied
    } else if as_fork_only.as_ref() == Some(&after) {
        ReconcileOutcome::ForkOnly
    } else {
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
    // touched branch.
    if as_model == as_with {
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
                let expect_with: Vec<(String, String)> =
                    with.state_of(branch).edges.union(g_with).cloned().collect();
                let expect_world: Vec<(String, String)> = world
                    .state_of(branch)
                    .edges
                    .union(g_world)
                    .cloned()
                    .collect();
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
        let expected_bound: Vec<(String, String)> = world
            .state_of(branch)
            .edges
            .union(&world.state_of(branch).ghosts)
            .cloned()
            .collect();
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
        // KNOWN CARVE-OUT: a version-collision merge failure is the op's own known
        // rejection, not the scheduled window being hit — counting it as a
        // crash would inflate hunt coverage. Reconcile anyway (the failed
        // merge dies at manifest commit and may raise the recovery barrier),
        // and a collided merge must never turn out durable.
        Err(err) if is_known_version_collision(wop, &err) => {
            let (db, outcome, _) = Box::pin(reconcile_after_failure(
                db,
                storage,
                root,
                wop,
                world,
                "version collision under scheduled window",
                at_op,
                recovery_crash,
            ))
            .await;
            if outcome != ReconcileOutcome::NotApplied {
                detectors::violation(
                    DET_ARBITRATION_QUERY,
                    at_op,
                    format!("version-collision merge reported failure yet arbitrated {outcome:?}"),
                    "a version-collision merge failure never becomes durable (NotApplied)",
                );
            }
            return (db, CrashOutcome::LegalRejection);
        }
        Err(_) => {}
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

        match init_result {
            Ok(db) => {
                assert!(usable(&db).await, "init survived {window} but store unusable");
                BirthOutcome::InitSurvived
            }
            Err(_) => {
                match Box::pin(Omnigraph::open_with_storage(root, storage.clone())).await {
                    Ok(db) => {
                        assert!(
                            usable(&db).await,
                            "reopen after {window} death succeeded but store unusable"
                        );
                        BirthOutcome::DiedThenOpensClean
                    }
                    Err(open_err) => {
                        let reopen = format!("{open_err:?}");
                        // RO must agree with RW — a split-brain birth state
                        // (RW refuses, RO serves) would be its own finding.
                        assert!(
                            Box::pin(Omnigraph::open_read_only_with_storage(
                                root,
                                storage.clone()
                            ))
                            .await
                            .is_err(),
                            "{window}: RW open refused but RO open SUCCEEDED (split-brain birth state)"
                        );
                        let reinit_result = Box::pin(Omnigraph::init_with_storage(
                            root,
                            TEST_SCHEMA,
                            storage.clone(),
                            InitOptions::default(),
                        ))
                        .await;
                        match reinit_result {
                            Ok(db) => {
                                assert!(
                                    usable(&db).await,
                                    "re-init after {window} death succeeded but store unusable"
                                );
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

/// Render a caught universe panic as a one-line repro-bearing message.
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
    // Seed logging: a failed CI run is reproducible from this line alone.
    println!(
        "dst universe [root={root} seed={} ops={} crash={:?} crash_on_match={:?} faults={} kill={:?}]",
        sc.seed,
        sc.ops,
        sc.crash_at,
        sc.crash_on_match,
        sc.faults.is_some(),
        sc.die_at_write
    );

    let mut seeds = SplitMix64(sc.seed);
    let runtime_seed = seeds.next_u64();
    let ulid_seed = seeds.next_u64();
    let workload_seed = seeds.next_u64();
    let entropy_seed = seeds.next_u64();

    unsafe { crate::env_knobs::quiesce() };

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
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("dst-universe".into())
            .stack_size(16 * 1024 * 1024)
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

        let base: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());
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
            if std::env::var("DST_OP_LOG").is_ok() {
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
                        match outcome {
                            ReconcileOutcome::Applied => apply_world(&mut world, &wop),
                            ReconcileOutcome::ForkOnly => apply_fork_only(&mut world, &wop),
                            ReconcileOutcome::NotApplied => {}
                        }
                        // a maintenance death gets its
                        // obligations judged, fault-suspended.
                        if let Some(f) = &failing {
                            f.suspend();
                        }
                        let ran = Box::pin(maintenance_obligations(
                            &mut db,
                            &world,
                            &wop,
                            &format!("crash:{failpoint}@op{i}"),
                            i,
                            sc.fail_maintenance_rerun,
                        ))
                        .await;
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
                reconcile_verdicts.push((
                    format!("crash-state:write#{}@op{i}", ks.writes_observed()),
                    format!("{outcome:?}"),
                    channel.to_string(),
                ));
                match outcome {
                    ReconcileOutcome::Applied => apply_world(&mut world, &wop),
                    ReconcileOutcome::ForkOnly => apply_fork_only(&mut world, &wop),
                    ReconcileOutcome::NotApplied => {}
                }
                // a maintenance death gets its obligations
                // judged, fault-suspended.
                if let Some(f) = &failing {
                    f.suspend();
                }
                let ran = Box::pin(maintenance_obligations(
                    &mut db,
                    &world,
                    &wop,
                    &format!("crash-state:write#{}@op{i}", ks.writes_observed()),
                    i,
                    sc.fail_maintenance_rerun,
                ))
                .await;
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
                        // succeed — dual-hypothesis assert (H-A/H-B live here).
                        if expected_conflict {
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
                        apply_world(&mut world, &wop);
                    }
                    Err(err) => {
                        // attributed detection: this op's reads
                        // crossed the damage ledger, so its (engine-born,
                        // unmarked) failure is the detection half of the
                        // detected-or-harmless contract — legal, recorded
                        // for typed-vs-raw triage. Detection quality is
                        // first-contact evidence, not a pass/fail axis yet.
                        let damaged_now = failing
                            .as_ref()
                            .map(|f| f.damage_events())
                            .unwrap_or(0)
                            > damage_before;
                        if damaged_now {
                            // Root-normalized: the report must stay
                            // root-independent so same-seed universes on
                            // different roots replay-compare equal.
                            let text = format!("{err:?}").replace(root, "<root>");
                            let snippet: String = text.chars().take(240).collect();
                            corruption_detections.push(format!("op{i} {wop:?}: {snippet}"));
                        }
                        if !(damaged_now
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
                        if is_known_version_collision(&wop, &err) {
                            known_issues.push(format!("#473-no-op-republish@op{i}"));
                        } else if is_recovery_barrier_rejection(&wop, &err) {
                            known_issues.push(format!("reopen-heals-barrier@op{i}"));
                        }
                        // CLIENT RETRY after ack-loss — semantics on
                        // `FaultPlan::client_retry`. Reconcile below still
                        // arbitrates the settled world either way.
                        if format!("{err:?}").contains(ACK_LOSS_MARKER)
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
                        if format!("{err:?}").contains(FAULT_MARKER)
                            || format!("{err:?}").contains(ACK_LOSS_MARKER)
                            // corruption-attributed failures (and
                            // marked latent sector errors, which advance the
                            // same ledger) MUST reconcile — the op may have
                            // durably written before its poisoned read, and
                            // only the two-picture arbitration can rule
                            // Applied vs NotApplied on a garbage-fed op.
                            || damaged_now
                            || is_known_version_collision(&wop, &err)
                            || is_recovery_barrier_rejection(&wop, &err)
                        {
                            // The engine arms recovery and bars
                            // writes until reopen — behave like a real client.
                            // (A version-collision merge failure dies at manifest commit
                            // too, so it gets the same reconcile+reopen.)
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
                            reconcile_verdicts.push((
                                format!("fault@op{i}"),
                                format!("{outcome:?}"),
                                channel.to_string(),
                            ));
                            match outcome {
                                ReconcileOutcome::Applied => apply_world(&mut world, &wop),
                                ReconcileOutcome::ForkOnly => apply_fork_only(&mut world, &wop),
                                ReconcileOutcome::NotApplied => {}
                            }
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
            .expect("read-path query")
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
    })
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
