//! concurrent multi-writer universes (reach: the interleaving
//! dimension).
//!
//! ENVELOPE (declared): **bite + oracles-hold**. N writer sessions
//! race on ONE shared store root from separate OS threads — the interleaving
//! is real scheduling, NOT seed-determined, so these universes carry NO
//! replay-identity claim (the lance-realm fault contract's precedent). Each
//! writer's OWN op stream is fully seed-determined: the `dst_ids`/`dst_clock`
//! seams are thread-local, so every writer thread installs its own with a
//! per-writer derived seed. A failure therefore yields the claim logs plus
//! the attributed serialization — a log, not a seed replay (#473-style
//! instrumented tracing is the minimization fallback). When the Lance
//! deterministic-mode PR closes the concurrent residual, this instrument
//! upgrades to strict replay without redesign.
//!
//! ORACLES — judgment without prediction. The model cannot foresee the
//! interleaving, so per-op equality is replaced by invariants that hold
//! under EVERY legal interleaving:
//!
//! 1. **Legality en route**: in a zero-fault universe the only legal write
//!    rejection is an OCC conflict (`kind: Conflict`); anything else reds
//!    inside the writer thread immediately.
//! 2. **OCC commit-id uniqueness** on main (world-level, unchanged).
//! 3. **The attributed serialization**: every write's value encodes
//!    `(writer, op)`, so re-reading every commit through the history
//!    surface (`ReadTarget::snapshot`) and
//!    diffing adjacent commits attributes each commit to exactly one
//!    claimed write. From that single reconstruction: no lost update
//!    (every claim-committed write appears exactly once), no phantom
//!    (a visible-but-unclaimed value is red; so is a double-apply),
//!    per-writer program order is preserved, and the EXACT final-state
//!    check — replaying the attributed order over the fixture base must
//!    reproduce the engine's final state. Strictly stronger than the
//!    originally specified contract "final state ∈ legal serialization outcomes":
//!    the history channel recovers THE serialization, so the legal set
//!    collapses to one predicted state.
//! 4. **Two-channel final audit**: the full read path (query channel) and
//!    the snapshot scan must agree on the final person rows.
//!
//! The judge (`judge_concurrent`) is a PURE function over recorded inputs,
//! so the seeded-blindness proofs feed it doctored logs (unit tests below),
//! each required to go red on its own check.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use omnigraph::db::{CleanupPolicyOptions, InitOptions, Omnigraph, ReadTarget, SnapshotId};
use omnigraph::loader::{LoadMode, load_jsonl};
use omnigraph::storage::{ObjectStorageAdapter, StorageAdapter};

use crate::fixtures::{
    MUTATION_QUERIES, TEST_DATA, TEST_SCHEMA, mixed_params, mutate_on, person_rows_target,
    query_main,
};
use crate::rand::SplitMix64;

/// Shared keys for the overlapping phase — the fixture's Person rows.
const SHARED_KEYS: [&str; 4] = ["Alice", "Bob", "Charlie", "Diana"];

/// Value encoding: age = (writer+1) * 100_000 + op#. Self-identifying, unique
/// across the universe, decodable from any channel. Fits I32 comfortably.
pub fn encode_value(writer: usize, op: usize) -> i64 {
    ((writer + 1) * 100_000 + op) as i64
}

/// Decode an encoded age back to (writer, op). Returns None for values that
/// are not writer-encoded (fixture ages).
pub fn decode_value(value: i64) -> Option<(usize, usize)> {
    if value < 100_000 {
        return None;
    }
    let writer = (value / 100_000) as usize - 1;
    let op = (value % 100_000) as usize;
    Some((writer, op))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimOutcome {
    /// The write path asserted success (possibly after legal OCC retries).
    Committed,
    /// ARM 2 — the op was in flight when its writer died: an INDEFINITE
    /// error (Jepsen sense — the operation may or may not have taken
    /// effect). The judge lets it appear in the serialization exactly once
    /// or not at all; nothing in between.
    Indeterminate,
}

/// One entry in a writer's claim log — the claim channel for this universe.
#[derive(Debug, Clone)]
pub struct ClaimedWrite {
    pub writer: usize,
    /// Program-order index within the writer.
    pub op: usize,
    pub key: String,
    pub value: i64,
    pub outcome: ClaimOutcome,
    /// Legal OCC conflicts absorbed before the claim landed.
    pub occ_retries: usize,
}

/// One writer-era commit, attributed to the claimed write that produced it.
/// Ordered oldest→newest = the true serialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributedCommit {
    pub commit_id: String,
    pub writer: usize,
    pub op: usize,
    pub key: String,
    pub value: i64,
}

#[derive(Debug, Clone)]
pub struct ConcurrentScenario {
    pub seed: u64,
    pub writers: usize,
    pub ops_per_writer: usize,
    /// ARM 1 — maintenance as a writer role: number of maintenance ops the
    /// dedicated maintenance actor draws (0 = no maintenance actor; its seed
    /// triple is drawn only when nonzero, so zero-knob scenarios keep their
    /// exact draw sequences). The actor races Optimize / Cleanup(keep=1) /
    /// ensure_indices against the data writers — the SQLite
    /// writer×checkpointer topology (Breaking-the-WAL prioritization
    /// signal).
    pub maintenance_ops: usize,
    /// ARM 2 — crash one writer mid-op: `(writer index, k)` kills that
    /// writer's adapter-realm storage at its k-th write-class call
    /// (mechanism: `WriterKillStorage`). Its in-flight op becomes an
    /// Indeterminate claim; the SURVIVORS keep racing and may now legally
    /// hit the recovery barrier (typed `RecoveryRequired` → reopen +
    /// retry). Envelope note: the kill is adapter-realm only — the dying
    /// op's Lance-realm writes may land (declared, not a hole: that IS a
    /// torn state for recovery to judge).
    pub kill_writer: Option<(usize, usize)>,
    /// ARM 3 — branch verbs under concurrency: a dedicated BRANCH ACTOR
    /// (writer id = `writers`, one past the data writers) runs this many
    /// fork→write→merge→delete cycles against main while the writers race —
    /// the verbs where the sequential suite's real findings live (#473's
    /// merge-publisher collision, the born-on-both fork-then-merge duplication,
    /// the read-corruption class's first-branch-write). Each cycle: `branch_create(cb<c>)`,
    /// 3 encoded person inserts ON the branch, `branch_merge(cb<c>, main)`,
    /// `branch_delete(cb<c>)`. Its writes are ordinary Committed claims:
    /// a merge that drops one is a LOST UPDATE red with no judge changes.
    pub branch_cycles: usize,
    /// READER ACTORS — nobody-reads-during-the-storm was a known blind spot:
    /// wrong-RESULT bugs that self-heal by quiesce (the schema-add poisoned
    /// reads, born-on-both path-dependent duplicates) are invisible to a
    /// final-audit-only judge. Each reader runs 2×ops_per_writer rounds of
    /// live reads; the four en-route oracles are documented on `reader_life`.
    pub readers: usize,
    /// FAULTS IN THE RACE (another known blind spot): that percentage of every
    /// NON-dying writer's storage calls (reads and writes — whole-call
    /// faults at the seam) fail with the fault marker, per-writer
    /// seeded streams. Writers retry marked faults like conflicts; a fault
    /// mid-op may arm recovery, so the barrier (`RecoveryRequired` →
    /// reopen-then-retry) is legal whenever this knob is on. The dying writer keeps
    /// its kill wrapper only, so death semantics stay clean.
    pub writer_fault_pct: u64,
    /// seam-granularity deterministic scheduling: route every MUTATING
    /// actor's storage calls — adapter realm, plus thread-attributed
    /// Lance-realm calls via the `lance_faults` shim — through ONE seeded
    /// serialization point (`SeamScheduler`), so the storage-visible
    /// interleaving becomes a function of the seed. Reader actors and
    /// unattributed Lance pool threads run ungated (counted:
    /// `sched_unattributed`). Scheduler seed = `seed ^ SEAM_SCHED_SALT`.
    /// The strict-replay claim holds iff the report's `sched_escapes == 0`.
    pub seam_schedule: bool,
    /// The DIRECTED HOLD, park-the-deleter form (the optimize-race recipe;
    /// requires `seam_schedule` and a branch actor): park the branch
    /// actor's ref-DELETE, spring it as the very next grant after a
    /// writer's listing of the branches dir (the TOCTOU sandwich).
    /// Protocol: `SchedState::hold_beneficiary`.
    pub park_deleter_hold: bool,
}

/// Writes per branch cycle (arm 3).
const BRANCH_WRITES_PER_CYCLE: usize = 3;

#[derive(Debug)]
pub struct ConcurrentReport {
    /// Claim-channel totals across writers.
    pub committed: usize,
    /// Legal OCC conflicts absorbed across all writers.
    pub occ_retries: usize,
    /// ARM 1 — maintenance actor's completed ops / its legal-conflict
    /// retries / how many of its ops were Cleanup (the horizon-mover).
    pub maintenance_committed: usize,
    pub maintenance_retries: usize,
    pub maintenance_cleanups: usize,
    /// Era commits whose person-diff is EMPTY — legal only with a
    /// maintenance actor, a dying/faulted writer's recovery pass, or a
    /// branch actor (none of these writes a writer-encoded person value).
    pub maintenance_commits: usize,
    /// Era commits legally unreadable at final audit because a concurrent
    /// Cleanup retired their versions (the retention horizon, live). The
    /// prefix-membership judge covers the claims that landed there.
    pub below_horizon: usize,
    /// ARM 3 — branch actor's totals: branch writes claimed committed,
    /// merges completed, legal-conflict retries across its verbs.
    pub branch_committed: usize,
    pub branch_merges: usize,
    pub branch_retries: usize,
    /// Reader-actor rounds completed (their oracles red by panic en route).
    pub reader_rounds: usize,
    /// Marked storage faults actually delivered to writers (bite evidence
    /// for the faults-in-the-race arm) and the retries they cost.
    pub writer_faults_injected: usize,
    pub fault_retries: usize,
    /// ISLANDS: contiguous readable runs the history walk saw (1 = the
    /// whole chain was readable; >1 = concurrent Cleanup carved gaps).
    pub islands: usize,
    /// ARM 2 — did the per-writer kill actually fire, and at which call;
    /// how many claims ended Indeterminate; how many survivor ops crossed
    /// the recovery barrier (typed RecoveryRequired → reopen + retry).
    pub dead_writer_hit: bool,
    pub dead_writer_label: Option<String>,
    pub indeterminate: usize,
    pub recovery_reopens: usize,
    /// INTERLEAVING EVIDENCE: adjacent attributed commits from different
    /// writers. 0 = the universe degenerated to sequential (threads never
    /// overlapped) — a vacuous green for the concurrency claim, so pins
    /// must demand ≥1 somewhere in their seed budget.
    pub alternations: usize,
    /// turns the arbiter granted / stall re-draws (the
    /// nondeterminism meter: 0 escapes = the storage-visible interleaving
    /// was fully seed-ordered, strict replay claimable) / the grant
    /// sequence itself (actor id per turn — the replay-diff artifact).
    /// All zero/empty when `seam_schedule` is off.
    pub sched_turns: usize,
    pub sched_escapes: usize,
    pub grant_log: Vec<usize>,
    /// Lance-realm gating: turns granted to
    /// attributed Lance-realm calls (they also appear in `grant_log`) /
    /// ungated calls from threads no actor owns (Lance pool threads) —
    /// the measured coverage gap of the thread-name attribution.
    pub sched_lance_turns: usize,
    pub sched_unattributed: usize,
    /// Directed-hold telemetry (all zero when `park_deleter_hold` is off): deletes
    /// parked / aligned springs delivered (each = one assembled
    /// list→delete→gets sandwich) / parks starved (no listing arrived).
    pub sched_holds: usize,
    pub sched_hold_released: usize,
    pub sched_hold_starved: usize,
    /// Writer-era commits on main, attributed oldest→newest.
    pub attributed: Vec<AttributedCommit>,
    /// Final (name, age) rows, sorted.
    pub end_state: Vec<(String, i64)>,
}

/// The pure judge. `base` is the person map at the last setup commit;
/// `attributed` is oldest→newest; `final_engine` is the engine's final
/// person map. Returns the first violated invariant as an error string.
pub fn judge_concurrent(
    claims: &[ClaimedWrite],
    attributed: &[AttributedCommit],
    base: &BTreeMap<String, i64>,
    final_engine: &BTreeMap<String, i64>,
) -> Result<(), String> {
    // (a)+(c) + the duplicate-claim guard: the standalone
    // attribution-consistency judge is the ONE home for those checks
    // (island universes run it alone; this judge adds (b) and (d)).
    judge_attribution_consistency(claims, attributed)?;
    let seen: BTreeSet<(usize, usize)> = attributed.iter().map(|a| (a.writer, a.op)).collect();

    // (b) no lost update: every COMMITTED claim must appear in the history
    //     (an Indeterminate claim is free to be absent — that is what
    //     indefinite means).
    for claim in claims {
        if claim.outcome == ClaimOutcome::Committed && !seen.contains(&(claim.writer, claim.op)) {
            return Err(format!(
                "LOST UPDATE: writer {} op {} ({}={}) was claimed committed but \
                 appears in no commit",
                claim.writer, claim.op, claim.key, claim.value
            ));
        }
    }

    // (d) exact final state: replay the attributed serialization over base.
    let mut replay = base.clone();
    for a in attributed {
        replay.insert(a.key.clone(), a.value);
    }
    if &replay != final_engine {
        return Err(format!(
            "FINAL STATE: replay of the attributed serialization diverges from the \
             engine.\n  replay: {replay:?}\n  engine: {final_engine:?}"
        ));
    }
    Ok(())
}

/// ARM 1 — the WEAKER judge for claims whose commits fell below the
/// retention horizon (a concurrent Cleanup retired their versions, so the
/// attributed-serialization walk cannot see them). This is exactly the
/// filed contract's original membership check, applied only where the
/// strong reconstruction is structurally blind. `s0` = the first readable
/// state after the horizon; `base` = the fixture state before any writer.
pub fn judge_prefix_membership(
    prefix_claims: &[ClaimedWrite],
    s0: &BTreeMap<String, i64>,
    base: &BTreeMap<String, i64>,
) -> Result<(), String> {
    // Index the prefix claims per key. Two candidate ledgers: each writer's
    // LAST committed value on the key, and its last value counting the
    // Indeterminate dying op too (which MAY have applied — indefinite).
    let mut all_values: BTreeSet<i64> = BTreeSet::new();
    let mut last_committed: BTreeMap<&str, BTreeMap<usize, i64>> = BTreeMap::new();
    let mut last_any: BTreeMap<&str, BTreeMap<usize, i64>> = BTreeMap::new();
    for c in prefix_claims {
        all_values.insert(c.value); // Indeterminate values MAY be visible
        last_any
            .entry(c.key.as_str())
            .or_default()
            .insert(c.writer, c.value); // ops iterate in program order per writer
        if c.outcome == ClaimOutcome::Committed {
            last_committed
                .entry(c.key.as_str())
                .or_default()
                .insert(c.writer, c.value);
        }
    }

    // (a) no phantom: every writer-encoded value visible at the horizon
    //     must be one of the prefix claims' values.
    for (key, value) in s0 {
        if decode_value(*value).is_some() && !all_values.contains(value) {
            return Err(format!(
                "PREFIX PHANTOM: horizon state carries {key}={value}, which no \
                 pre-horizon claim wrote"
            ));
        }
    }
    for (key, any_candidates) in &last_any {
        let committed_candidates = last_committed.get(key);
        match s0.get(*key) {
            None => {
                // Key absence is legal only if nothing DEFINITE created it.
                if committed_candidates.is_some() {
                    return Err(format!(
                        "PREFIX LOST KEY: {key} had pre-horizon committed writes but \
                         is absent at the horizon"
                    ));
                }
            }
            Some(v) => {
                let base_v = base.get(*key);
                if decode_value(*v).is_some() {
                    // (b) last-candidate membership: the surviving value must be
                    //     some writer's LAST pre-horizon write on the key —
                    //     committed for sure, or the indefinite dying op.
                    let legal = committed_candidates
                        .map(|m| m.values().any(|c| c == v))
                        .unwrap_or(false)
                        || any_candidates.values().any(|c| c == v);
                    if !legal {
                        return Err(format!(
                            "PREFIX ORDER: horizon state {key}={v} is a pre-horizon \
                             value that no writer wrote LAST (candidates \
                             {any_candidates:?}) — an intermediate write survived"
                        ));
                    }
                } else if base_v == Some(v) && committed_candidates.is_some() {
                    // (c) lost update: COMMITTED pre-horizon writes exist on the
                    //     key, yet the horizon still shows the fixture value.
                    //     (Indeterminate-only traffic showing base is legal.)
                    return Err(format!(
                        "PREFIX LOST UPDATE: {key} shows its base value {v} at the \
                         horizon despite pre-horizon committed writes \
                         {committed_candidates:?}"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The attribution-consistency half of `judge_concurrent`, standalone for
/// ISLAND universes (concurrent Cleanup makes history readability
/// non-contiguous — readable islands between horizons — so the full judge's
/// lost-update and replay checks can't run per-commit; those move to the
/// head-membership judge, which is `judge_prefix_membership` applied to the
/// FINAL state). Checks: every attributed commit is owned by a claim and
/// matches it, no double-apply, per-writer program order along the
/// serialization.
pub fn judge_attribution_consistency(
    claims: &[ClaimedWrite],
    attributed: &[AttributedCommit],
) -> Result<(), String> {
    let mut claim_index: BTreeMap<(usize, usize), &ClaimedWrite> = BTreeMap::new();
    for c in claims {
        if claim_index.insert((c.writer, c.op), c).is_some() {
            return Err(format!(
                "claim log corrupt: duplicate claim for writer {} op {}",
                c.writer, c.op
            ));
        }
    }
    let mut seen: BTreeSet<(usize, usize)> = BTreeSet::new();
    let mut last_op: BTreeMap<usize, usize> = BTreeMap::new();
    for a in attributed {
        let Some(claim) = claim_index.get(&(a.writer, a.op)) else {
            return Err(format!(
                "VISIBLE-BUT-UNCLAIMED: commit {} carries writer {} op {} which no \
                 claim owns",
                a.commit_id, a.writer, a.op
            ));
        };
        if claim.key != a.key || claim.value != a.value {
            return Err(format!(
                "ATTRIBUTION MISMATCH: commit {} says ({}, {}) wrote {}={} but the \
                 claim says {}={}",
                a.commit_id, a.writer, a.op, a.key, a.value, claim.key, claim.value
            ));
        }
        if !seen.insert((a.writer, a.op)) {
            return Err(format!(
                "DOUBLE-APPLY: writer {} op {} attributed to more than one commit",
                a.writer, a.op
            ));
        }
        if let Some(prev) = last_op.get(&a.writer)
            && a.op <= *prev
        {
            return Err(format!(
                "PROGRAM ORDER: writer {} op {} serialized after its own op {}",
                a.writer, a.op, prev
            ));
        }
        last_op.insert(a.writer, a.op);
    }
    Ok(())
}

/// See [`SeamScheduler::finish_on_drop`].
pub struct FinishOnDrop {
    sched: Arc<SeamScheduler>,
    actor: usize,
}

impl Drop for FinishOnDrop {
    fn drop(&mut self) {
        self.sched.finish(self.actor);
        omnigraph::dst_gate::uninstall_gate_hook();
    }
}

fn person_map(rows: &[(String, i64, i64)]) -> BTreeMap<String, i64> {
    rows.iter().map(|(n, a, _)| (n.clone(), *a)).collect()
}

/// Fallible history read — like `person_rows_target` but returns the error
/// instead of panicking, because below the retention horizon a commit's
/// snapshot is LEGALLY unreadable (the retention-horizon finding, now live under a
/// concurrent Cleanup).
async fn try_person_map(db: &Omnigraph, commit_id: &str) -> Result<BTreeMap<String, i64>, String> {
    use arrow_array::{Array, Int32Array, StringArray};
    use futures::TryStreamExt;
    let snap = db
        .snapshot_of(ReadTarget::snapshot(SnapshotId::new(commit_id)))
        .await
        .map_err(|e| format!("{e:?}"))?;
    let ds = snap
        .open_dataset("node:Person")
        .await
        .map_err(|e| format!("{e:?}"))?;
    let batches: Vec<arrow_array::RecordBatch> = ds
        .scan()
        .try_into_stream()
        .await
        .map_err(|e| format!("{e:?}"))?
        .try_collect()
        .await
        .map_err(|e| format!("{e:?}"))?;
    let mut map = BTreeMap::new();
    for batch in batches {
        let names = batch
            .column_by_name("name")
            .ok_or("no name column")?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or("name not a StringArray")?
            .clone();
        let ages = batch
            .column_by_name("age")
            .ok_or("no age column")?
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or("age not an Int32Array")?
            .clone();
        for i in 0..names.len() {
            if names.is_valid(i) {
                let age = if ages.is_valid(i) {
                    ages.value(i) as i64
                } else {
                    -1
                };
                map.insert(names.value(i).to_string(), age);
            }
        }
    }
    Ok(map)
}

/// ARM 2 — per-writer kill-at-kth-write storage: wraps ONE writer's view of
/// the shared adapter; at that writer's k-th write-class call the wrapper
/// goes DEAD — the call and every later call (reads included) fail with the
/// kill marker, no revive. The one-participant process-death analog: peers'
/// storage is untouched, and whatever the dying op had already landed stays
/// as residue for the survivors and recovery to meet.
#[derive(Debug)]
pub struct WriterKillStorage {
    inner: Arc<dyn StorageAdapter>,
    die_at: usize,
    writes: std::sync::atomic::AtomicUsize,
    dead: std::sync::atomic::AtomicBool,
    killed_label: std::sync::Mutex<Option<String>>,
}

impl WriterKillStorage {
    pub fn new(inner: Arc<dyn StorageAdapter>, die_at: usize) -> Self {
        Self {
            inner,
            die_at: die_at.max(1),
            writes: std::sync::atomic::AtomicUsize::new(0),
            dead: std::sync::atomic::AtomicBool::new(false),
            killed_label: std::sync::Mutex::new(None),
        }
    }
    pub fn dead(&self) -> bool {
        self.dead.load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn killed_label(&self) -> Option<String> {
        self.killed_label.lock().unwrap().clone()
    }
    /// Post-mortem refusal on every call; death fires on the k-th write.
    fn gate(&self, op: &str, uri: &str, is_write: bool) -> omnigraph::error::Result<()> {
        use omnigraph::error::OmniError;
        let marker = crate::harness::KILL_MARKER;
        if self.dead() {
            return Err(OmniError::manifest(format!(
                "{marker}: post-mortem {op} {uri}"
            )));
        }
        if is_write {
            let n = self
                .writes
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            if n == self.die_at {
                self.dead.store(true, std::sync::atomic::Ordering::SeqCst);
                *self.killed_label.lock().unwrap() = Some(format!("{op} {uri}"));
                return Err(OmniError::manifest(format!(
                    "{marker}: write #{n} {op} {uri}"
                )));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageAdapter for WriterKillStorage {
    async fn read_text(&self, uri: &str) -> omnigraph::error::Result<String> {
        self.gate("read_text", uri, false)?;
        self.inner.read_text(uri).await
    }
    async fn read_text_if_exists(&self, uri: &str) -> omnigraph::error::Result<Option<String>> {
        self.gate("read_text_if_exists", uri, false)?;
        self.inner.read_text_if_exists(uri).await
    }
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> omnigraph::error::Result<Option<String>> {
        self.gate("read_text_if_exists_bounded", uri, false)?;
        self.inner.read_text_if_exists_bounded(uri, max_bytes).await
    }
    async fn write_text(&self, uri: &str, contents: &str) -> omnigraph::error::Result<()> {
        self.gate("write_text", uri, true)?;
        self.inner.write_text(uri, contents).await
    }
    async fn write_text_if_absent(
        &self,
        uri: &str,
        contents: &str,
    ) -> omnigraph::error::Result<bool> {
        self.gate("write_text_if_absent", uri, true)?;
        self.inner.write_text_if_absent(uri, contents).await
    }
    async fn exists(&self, uri: &str) -> omnigraph::error::Result<bool> {
        self.gate("exists", uri, false)?;
        self.inner.exists(uri).await
    }
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> omnigraph::error::Result<()> {
        self.gate("rename_text", from_uri, true)?;
        self.inner.rename_text(from_uri, to_uri).await
    }
    async fn delete(&self, uri: &str) -> omnigraph::error::Result<()> {
        self.gate("delete", uri, true)?;
        self.inner.delete(uri).await
    }
    async fn list_dir(&self, dir_uri: &str) -> omnigraph::error::Result<Vec<String>> {
        self.gate("list_dir", dir_uri, false)?;
        self.inner.list_dir(dir_uri).await
    }
    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: omnigraph::storage::ListDirBounds,
    ) -> omnigraph::error::Result<Vec<String>> {
        self.gate("list_dir_bounded", dir_uri, false)?;
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }
    async fn read_text_versioned(&self, uri: &str) -> omnigraph::error::Result<(String, String)> {
        self.gate("read_text_versioned", uri, false)?;
        self.inner.read_text_versioned(uri).await
    }
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> omnigraph::error::Result<Option<String>> {
        self.gate("write_text_if_match", uri, true)?;
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> omnigraph::error::Result<()> {
        self.gate("delete_prefix", prefix_uri, true)?;
        self.inner.delete_prefix(prefix_uri).await
    }
}

/// FAULTS IN THE RACE — per-writer whole-call fault wrapper: a seeded
/// percentage of this writer's storage calls (reads AND writes) fail with
/// the fault marker before delegation. Per-writer streams keep each
/// writer's fault weather its own; the interleaving stays the universe's.
#[derive(Debug)]
pub struct WriterFaultStorage {
    inner: Arc<dyn StorageAdapter>,
    rng: std::sync::Mutex<SplitMix64>,
    error_pct: u64,
    injected: std::sync::atomic::AtomicUsize,
    /// Faults roll only once ARMED — the writer arms after its handle opens
    /// and the start barrier releases. A fault during the OPEN panics that
    /// writer thread BEFORE the barrier and deadlocks every other
    /// participant on it (measured: the first storm-arm run hung the whole
    /// fleet test); FailingStorage's `enabled` flag exists for exactly this
    /// reason, and this mirrors it.
    armed: std::sync::atomic::AtomicBool,
}

impl WriterFaultStorage {
    pub fn new(inner: Arc<dyn StorageAdapter>, seed: u64, error_pct: u64) -> Self {
        Self {
            inner,
            rng: std::sync::Mutex::new(SplitMix64(seed)),
            error_pct: error_pct.min(95), // progress must stay possible
            injected: std::sync::atomic::AtomicUsize::new(0),
            armed: std::sync::atomic::AtomicBool::new(false),
        }
    }
    pub fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    pub fn injected(&self) -> usize {
        self.injected.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn roll(&self, op: &str, uri: &str) -> omnigraph::error::Result<()> {
        if !self.armed.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }
        let draw = { self.rng.lock().unwrap().next_u64() % 100 };
        if draw < self.error_pct {
            self.injected
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Err(omnigraph::error::OmniError::manifest(format!(
                "{}: {op} {uri}",
                crate::harness::FAULT_MARKER
            )));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageAdapter for WriterFaultStorage {
    async fn read_text(&self, uri: &str) -> omnigraph::error::Result<String> {
        self.roll("read_text", uri)?;
        self.inner.read_text(uri).await
    }
    async fn read_text_if_exists(&self, uri: &str) -> omnigraph::error::Result<Option<String>> {
        self.roll("read_text_if_exists", uri)?;
        self.inner.read_text_if_exists(uri).await
    }
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> omnigraph::error::Result<Option<String>> {
        self.roll("read_text_if_exists_bounded", uri)?;
        self.inner.read_text_if_exists_bounded(uri, max_bytes).await
    }
    async fn write_text(&self, uri: &str, contents: &str) -> omnigraph::error::Result<()> {
        self.roll("write_text", uri)?;
        self.inner.write_text(uri, contents).await
    }
    async fn write_text_if_absent(
        &self,
        uri: &str,
        contents: &str,
    ) -> omnigraph::error::Result<bool> {
        self.roll("write_text_if_absent", uri)?;
        self.inner.write_text_if_absent(uri, contents).await
    }
    async fn exists(&self, uri: &str) -> omnigraph::error::Result<bool> {
        self.roll("exists", uri)?;
        self.inner.exists(uri).await
    }
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> omnigraph::error::Result<()> {
        self.roll("rename_text", from_uri)?;
        self.inner.rename_text(from_uri, to_uri).await
    }
    async fn delete(&self, uri: &str) -> omnigraph::error::Result<()> {
        self.roll("delete", uri)?;
        self.inner.delete(uri).await
    }
    async fn list_dir(&self, dir_uri: &str) -> omnigraph::error::Result<Vec<String>> {
        self.roll("list_dir", dir_uri)?;
        self.inner.list_dir(dir_uri).await
    }
    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: omnigraph::storage::ListDirBounds,
    ) -> omnigraph::error::Result<Vec<String>> {
        self.roll("list_dir_bounded", dir_uri)?;
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }
    async fn read_text_versioned(&self, uri: &str) -> omnigraph::error::Result<(String, String)> {
        self.roll("read_text_versioned", uri)?;
        self.inner.read_text_versioned(uri).await
    }
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> omnigraph::error::Result<Option<String>> {
        self.roll("write_text_if_match", uri)?;
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> omnigraph::error::Result<()> {
        self.roll("delete_prefix", prefix_uri)?;
        self.inner.delete_prefix(prefix_uri).await
    }
}

/// the seam-granularity deterministic scheduler: ONE seeded
/// serialization point (the arbiter) behind every mutating actor's storage
/// wrapper.
///
/// Turns name ACTORS, not calls: a seeded draw picks which LIVE actor's
/// next pending storage call executes now. You cannot pre-name an actor's
/// k-th call (its future calls depend on what the schedule lets it
/// observe), and you don't need to — determinism follows by INDUCTION on
/// the grant prefix: same seed ⇒ same actor sequence; given an identical
/// prefix every actor has observed identical history, sits in an identical
/// state, and presents the SAME next call. The total order over turns
/// generates the total order over calls.
///
/// The draw is NEVER over the arrival set — "whoever happens to be waiting"
/// is OS timing, and choosing among arrivals would re-import the
/// nondeterminism through the choice set (the lance-realm fault decisions'
/// global-arrival-order bug, one level up). The arbiter draws from the
/// LIVE set (registered, not finished — both prefix-deterministic) and
/// WAITS for the drawn actor. Only a real-time stall (drawn actor parked
/// on an in-process lock, or off in ungated Lance-realm compute) re-draws
/// over the pending set — deliberately nondeterministic, deliberately
/// counted: `escapes` is the declared nondeterminism meter, and the
/// STRICT-REPLAY CLAIM HOLDS IFF escapes == 0. Any escape degrades that
/// universe to bite+oracles-hold — recorded, never silent.
///
/// Armed only after the start barrier (a gated call during a handle OPEN
/// would deadlock the barrier — `WriterFaultStorage::arm`'s measured
/// lesson); setup and the final audit run on the raw adapter, ungated.
pub struct SeamScheduler {
    state: std::sync::Mutex<SchedState>,
    cv: std::sync::Condvar,
    /// Lance-realm gating telemetry: turns granted to
    /// attributed Lance-realm calls / calls that arrived on threads no
    /// actor owns (Lance pool threads, the setup/audit thread) and ran
    /// UNGATED. Unattributed calls consume no draws, so they cannot
    /// perturb the seeded schedule — they are the measured coverage gap.
    lance_turns: std::sync::atomic::AtomicUsize,
    unattributed: std::sync::atomic::AtomicUsize,
}

struct SchedState {
    rng: SplitMix64,
    /// Registered actors that have not finished — the draw domain.
    live: BTreeSet<usize>,
    /// Actors currently waiting at the gate (arrival set — escape-only).
    pending: BTreeSet<usize>,
    /// A granted call is in flight; the turn is the critical section.
    executing: bool,
    /// The drawn actor the arbiter is waiting for (None = draw on wake).
    scheduled: Option<usize>,
    grant_log: Vec<usize>,
    escapes: usize,
    enabled: bool,
    last_progress: std::time::Instant,
    /// DIRECTED HOLD, park-the-deleter form (supersedes park-the-writer
    /// — release on any beneficiary refs call — which measured 150
    /// releases / 0 sandwiches because deletes are ~6 of the
    /// beneficiary's ~2,000 refs calls). When armed with the
    /// beneficiary's id (the branch actor): the beneficiary's ref-DELETE
    /// (`op=delete`, `_refs/branches/…json`) is PARKED at the gate —
    /// excluded from draws — until any writer's LISTING of the branches
    /// dir completes; the delete is then SPRUNG as the very next grant,
    /// landing exactly between that writer's list and its per-branch gets.
    /// A park that no listing rescues within the starve budget is released
    /// uncounted-as-aligned (starved).
    hold_beneficiary: Option<usize>,
    /// The parked deleter (the beneficiary, while its delete waits).
    parked: Option<usize>,
    park_start: std::time::Instant,
    /// Set at a victim list's GRANT while a deleter is parked; its guard
    /// drop performs the spring.
    spring_on_drop: bool,
    /// The parked delete may proceed (set by the spring or by starvation).
    spring: bool,
    parks: usize,
    aligned: usize,
    park_starved: usize,
}

/// Salted off the universe seed (`seed ^ SEAM_SCHED_SALT`) so enabling the
/// scheduler never shifts the main stream's draw sequences (the
/// lance-realm-salt pattern).
pub const SEAM_SCHED_SALT: u64 = 0x5EAA_5C4E_D001_0032;
/// How long the arbiter waits for the drawn actor before an escape re-draw.
/// With the `dst_gate` seam, engine-gate contenders spin at the arbiter, so
/// a drawn actor is always pending or microseconds away and escapes are 0
/// by construction; the budget prices only true wedges and rare OS
/// preemption pauses. Generous on purpose — a 5 ms budget once fired a
/// spurious escape under full-suite load.
const GRANT_STALL_BUDGET: std::time::Duration = std::time::Duration::from_millis(400);
/// Condvar wake tick while waiting for a turn.
const GRANT_WAIT_TICK: std::time::Duration = std::time::Duration::from_millis(5);
/// Directed hold: how long a victim stays parked waiting for the
/// beneficiary before the hold is released as STARVED (the measured
/// verdict that the beneficiary cannot reach its refs call while the
/// victim is mid-window — engine-gate masking).
const HOLD_STARVE_BUDGET: std::time::Duration = std::time::Duration::from_millis(300);
/// Directed hold: cap per universe (bounds worst-case wall time).
const DIRECTED_HOLD_CAP: usize = 40;

impl SeamScheduler {
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            state: std::sync::Mutex::new(SchedState {
                rng: SplitMix64(seed),
                live: BTreeSet::new(),
                pending: BTreeSet::new(),
                executing: false,
                scheduled: None,
                grant_log: Vec::new(),
                escapes: 0,
                enabled: false,
                last_progress: std::time::Instant::now(),
                hold_beneficiary: None,
                parked: None,
                park_start: std::time::Instant::now(),
                spring_on_drop: false,
                spring: false,
                parks: 0,
                aligned: 0,
                park_starved: 0,
            }),
            cv: std::sync::Condvar::new(),
            lance_turns: std::sync::atomic::AtomicUsize::new(0),
            unattributed: std::sync::atomic::AtomicUsize::new(0),
        })
    }
    pub fn register(&self, actor: usize) {
        self.state.lock().unwrap().live.insert(actor);
    }
    pub(crate) fn note_lance_turn(&self) {
        self.lance_turns
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    pub(crate) fn note_unattributed(&self) {
        self.unattributed
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    /// (Lance-realm turns granted, unattributed ungated calls).
    pub fn lance_stats(&self) -> (usize, usize) {
        (
            self.lance_turns.load(std::sync::atomic::Ordering::SeqCst),
            self.unattributed.load(std::sync::atomic::Ordering::SeqCst),
        )
    }
    /// Idempotent; every participant calls it right after the start barrier.
    pub fn arm(&self) {
        let mut st = self.state.lock().unwrap();
        if !st.enabled {
            st.enabled = true;
            st.last_progress = std::time::Instant::now();
        }
        drop(st);
        self.cv.notify_all();
    }
    /// The actor's life is over: it will never present another call.
    /// Leaving the live set is prefix-deterministic (the actor's whole
    /// behavior is), so draws skip it without breaking the induction.
    pub fn finish(&self, actor: usize) {
        let mut st = self.state.lock().unwrap();
        st.live.remove(&actor);
        st.pending.remove(&actor);
        if st.scheduled == Some(actor) {
            st.scheduled = None; // re-draw on the next wake
        }
        if st.parked == Some(actor) {
            st.parked = None; // a finished actor cannot stay parked
        }
        drop(st);
        self.cv.notify_all();
    }
    pub fn snapshot(&self) -> (Vec<usize>, usize) {
        let st = self.state.lock().unwrap();
        (st.grant_log.clone(), st.escapes)
    }

    /// RAII `finish` for an actor life: dropping the guard (normal exit OR
    /// a panicking oracle red) removes the actor from the draw domain and
    /// uninstalls its gate hook. Without the panic path, a dead gated
    /// actor stays drawable and every later draw naming it stalls into a
    /// counted escape — polluting exactly the failing universe's
    /// evidence. Declare AFTER the actor's `db` handle so finish runs
    /// before the handle drop (drop order is reverse declaration).
    pub fn finish_on_drop(self: &Arc<Self>, actor: usize) -> FinishOnDrop {
        FinishOnDrop {
            sched: Arc::clone(self),
            actor,
        }
    }
    /// Arm the DIRECTED HOLD (the park-the-deleter recipe): `beneficiary` = the branch
    /// actor's scheduler id. See `SchedState`'s field doc for the protocol.
    pub fn arm_directed_hold(&self, beneficiary: usize) {
        self.state.lock().unwrap().hold_beneficiary = Some(beneficiary);
    }
    /// (deletes parked, aligned springs delivered, parks starved).
    pub fn hold_stats(&self) -> (usize, usize, usize) {
        let st = self.state.lock().unwrap();
        (st.parks, st.aligned, st.park_starved)
    }
    fn draw(st: &mut SchedState) -> Option<usize> {
        // The parked deleter is excluded from the draw domain until sprung
        // (draw-count divergence vs hold-off runs is fine — the hold IS a
        // different schedule regime, declared by the knob).
        let candidates: Vec<usize> = st
            .live
            .iter()
            .copied()
            .filter(|a| st.parked != Some(*a))
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let n = (st.rng.next_u64() % candidates.len() as u64) as usize;
        Some(candidates[n])
    }
    /// Metadata-free turn acquisition (the adapter realm's path).
    pub(crate) fn enter(self: &Arc<Self>, actor: usize) -> Option<SeamGuard> {
        self.enter_call(actor, "", "")
    }
    /// Blocking turn acquisition (std sync is fine: each actor's
    /// current_thread runtime runs nothing else — the start-barrier
    /// precedent at `writer_life`). Returns `None` while unarmed
    /// (setup/audit run ungated); otherwise a guard holding the turn for
    /// exactly one delegated call. `pub(crate)`: the Lance-realm shim
    /// (`lance_faults`) takes turns through the same arbiter, passing the
    /// call's (op, location) so the directed hold can pattern-match refs
    /// traffic.
    pub(crate) fn enter_call(
        self: &Arc<Self>,
        actor: usize,
        _op: &str,
        location: &str,
    ) -> Option<SeamGuard> {
        let mut st = self.state.lock().unwrap();
        if !st.enabled {
            return None;
        }
        st.pending.insert(actor);
        loop {
            // Park starvation: no writer listing arrived to spring the
            // parked delete — let it through unsprung and count (any
            // waiter's loop performs this check on its wake tick, the
            // parked deleter included).
            if st.parked.is_some() && st.park_start.elapsed() >= HOLD_STARVE_BUDGET {
                st.parked = None;
                st.spring = true; // proceed without the sandwich
                st.park_starved += 1;
                self.cv.notify_all();
            }
            // A scheduled actor that just got parked must be re-drawn.
            if st.scheduled.is_some() && st.scheduled == st.parked {
                st.scheduled = None;
            }
            if st.scheduled.is_none() && !st.executing {
                st.scheduled = Self::draw(&mut st);
                st.last_progress = std::time::Instant::now();
                self.cv.notify_all();
            }
            if st.scheduled == Some(actor) && !st.executing && st.parked != Some(actor) {
                // PARK-THE-DELETER: the beneficiary's ref-delete announces
                // itself here — park it instead of granting, until a
                // writer's listing springs it (or starvation).
                if !st.spring
                    && st.hold_beneficiary == Some(actor)
                    && _op == "delete"
                    && location.contains("_refs/branches/")
                    && st.parks < DIRECTED_HOLD_CAP
                {
                    st.parked = Some(actor);
                    st.park_start = std::time::Instant::now();
                    st.parks += 1;
                    st.scheduled = None; // the turn goes to someone else
                    self.cv.notify_all();
                    let (guard, _) = self.cv.wait_timeout(st, GRANT_WAIT_TICK).unwrap();
                    st = guard;
                    continue;
                }
                st.executing = true;
                st.scheduled = None;
                st.pending.remove(&actor);
                st.grant_log.push(actor);
                st.last_progress = std::time::Instant::now();
                if st.spring && st.hold_beneficiary == Some(actor) {
                    // The sprung delete is being granted — the sandwich is
                    // assembled; further deletes may park again.
                    st.spring = false;
                }
                // A writer's LISTING of the branches dir completing while a
                // delete is parked = the spring trigger (fired at this
                // turn's release, so the delete lands right AFTER the list).
                if st.parked.is_some()
                    && st.hold_beneficiary != Some(actor)
                    && _op == "list_with_delimiter"
                    && location.ends_with("_refs/branches")
                {
                    st.spring_on_drop = true;
                }
                return Some(SeamGuard {
                    sched: self.clone(),
                });
            }
            let (guard, timeout) = self.cv.wait_timeout(st, GRANT_WAIT_TICK).unwrap();
            st = guard;
            if timeout.timed_out()
                && !st.executing
                && let Some(s) = st.scheduled
                && !st.pending.contains(&s)
                && st.last_progress.elapsed() >= GRANT_STALL_BUDGET
            {
                // ESCAPE: the drawn actor is stalled outside the gate.
                // Re-draw over the ARRIVAL set (the parked deleter
                // excluded) — deliberately nondeterministic, deliberately
                // counted.
                let pool: Vec<usize> = st
                    .pending
                    .iter()
                    .copied()
                    .filter(|a| st.parked != Some(*a))
                    .collect();
                if !pool.is_empty() {
                    let n = (st.rng.next_u64() % pool.len() as u64) as usize;
                    st.scheduled = Some(pool[n]);
                    st.escapes += 1;
                    st.last_progress = std::time::Instant::now();
                    self.cv.notify_all();
                }
            }
        }
    }
}

/// Holds the turn for exactly one delegated call; dropping it releases the
/// serialization point and wakes the waiters.
pub struct SeamGuard {
    sched: Arc<SeamScheduler>,
}
impl Drop for SeamGuard {
    fn drop(&mut self) {
        let mut st = self.sched.state.lock().unwrap();
        st.executing = false;
        st.last_progress = std::time::Instant::now();
        // The SPRING happens at the victim list's RELEASE: the parked
        // delete becomes the very next grant, landing exactly between the
        // completed list and the walk's per-branch gets.
        if st.spring_on_drop {
            st.spring_on_drop = false;
            if let Some(deleter) = st.parked.take() {
                st.scheduled = Some(deleter);
                st.spring = true;
                st.aligned += 1;
            }
        }
        drop(st);
        self.sched.cv.notify_all();
    }
}

/// The gate a mutating actor's storage view passes through: OUTERMOST
/// wrapper (over kill/fault wrappers), one delegated call per granted turn.
pub struct ScheduledStorage {
    inner: Arc<dyn StorageAdapter>,
    sched: Arc<SeamScheduler>,
    actor: usize,
}
impl std::fmt::Debug for ScheduledStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ScheduledStorage(actor={})", self.actor)
    }
}
impl ScheduledStorage {
    pub fn new(inner: Arc<dyn StorageAdapter>, sched: Arc<SeamScheduler>, actor: usize) -> Self {
        Self {
            inner,
            sched,
            actor,
        }
    }
}

#[async_trait::async_trait]
impl StorageAdapter for ScheduledStorage {
    async fn read_text(&self, uri: &str) -> omnigraph::error::Result<String> {
        let _turn = self.sched.enter(self.actor);
        self.inner.read_text(uri).await
    }
    async fn read_text_if_exists(&self, uri: &str) -> omnigraph::error::Result<Option<String>> {
        let _turn = self.sched.enter(self.actor);
        self.inner.read_text_if_exists(uri).await
    }
    async fn read_text_if_exists_bounded(
        &self,
        uri: &str,
        max_bytes: u64,
    ) -> omnigraph::error::Result<Option<String>> {
        let _turn = self.sched.enter(self.actor);
        self.inner.read_text_if_exists_bounded(uri, max_bytes).await
    }
    async fn write_text(&self, uri: &str, contents: &str) -> omnigraph::error::Result<()> {
        let _turn = self.sched.enter(self.actor);
        self.inner.write_text(uri, contents).await
    }
    async fn write_text_if_absent(
        &self,
        uri: &str,
        contents: &str,
    ) -> omnigraph::error::Result<bool> {
        let _turn = self.sched.enter(self.actor);
        self.inner.write_text_if_absent(uri, contents).await
    }
    async fn exists(&self, uri: &str) -> omnigraph::error::Result<bool> {
        let _turn = self.sched.enter(self.actor);
        self.inner.exists(uri).await
    }
    async fn rename_text(&self, from_uri: &str, to_uri: &str) -> omnigraph::error::Result<()> {
        let _turn = self.sched.enter(self.actor);
        self.inner.rename_text(from_uri, to_uri).await
    }
    async fn delete(&self, uri: &str) -> omnigraph::error::Result<()> {
        let _turn = self.sched.enter(self.actor);
        self.inner.delete(uri).await
    }
    async fn list_dir(&self, dir_uri: &str) -> omnigraph::error::Result<Vec<String>> {
        let _turn = self.sched.enter(self.actor);
        self.inner.list_dir(dir_uri).await
    }
    async fn list_dir_bounded(
        &self,
        dir_uri: &str,
        matching_suffix: &str,
        bounds: omnigraph::storage::ListDirBounds,
    ) -> omnigraph::error::Result<Vec<String>> {
        let _turn = self.sched.enter(self.actor);
        self.inner
            .list_dir_bounded(dir_uri, matching_suffix, bounds)
            .await
    }
    async fn read_text_versioned(&self, uri: &str) -> omnigraph::error::Result<(String, String)> {
        let _turn = self.sched.enter(self.actor);
        self.inner.read_text_versioned(uri).await
    }
    async fn write_text_if_match(
        &self,
        uri: &str,
        contents: &str,
        expected_version: &str,
    ) -> omnigraph::error::Result<Option<String>> {
        let _turn = self.sched.enter(self.actor);
        self.inner
            .write_text_if_match(uri, contents, expected_version)
            .await
    }
    async fn delete_prefix(&self, prefix_uri: &str) -> omnigraph::error::Result<()> {
        let _turn = self.sched.enter(self.actor);
        self.inner.delete_prefix(prefix_uri).await
    }
}

/// Per-writer context: (own kill wrapper if dying, RecoveryRequired-is-legal
/// — true when any peer can die or faults are injected, own fault wrapper to
/// ARM after the barrier, the universe's arbiter — armed after
/// the barrier and finished when the life ends).
type WriterCtx = (
    Option<Arc<WriterKillStorage>>,
    bool,
    Option<Arc<WriterFaultStorage>>,
    Option<Arc<SeamScheduler>>,
);

/// One writer's whole life: its own OS thread, runtime, seams, handle, and
/// seeded op stream against the shared root. Returns (claim log, recovery
/// reopens, fault retries). `kill_ctx`: see `WriterCtx`.
fn writer_life(
    root: &str,
    storage: Arc<dyn StorageAdapter>,
    writer: usize,
    ops: usize,
    // (runtime, ulid, workload) — this writer's derived seed triple.
    seeds3: (u64, u64, u64),
    start: Arc<std::sync::Barrier>,
    kill_ctx: WriterCtx,
) -> (Vec<ClaimedWrite>, usize, usize) {
    let (runtime_seed, ulid_seed, workload_seed) = seeds3;
    let (kill, recovery_legal, fault, sched) = kill_ctx;
    // ThreadRng is thread-local: reseed inside this thread.
    let _ = rand::rng().reseed();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        // REAL time: two runtimes cannot share one virtual clock, and this
        // universe holds no replay claim anyway (envelope above).
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("writer runtime");
    runtime.block_on(Box::pin(async move {
        // Thread-local seams: this writer's ULIDs/timestamps are its own
        // seeded stream — deterministic per writer, interleaving aside.
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        // The write-gate seam (`omnigraph::dst_gate`'s doc has the why):
        // gate acquisition takes turns through the same arbiter; unarmed,
        // the hook returns None (plain blocking locks).
        if let Some(s) = &sched {
            let hook_sched = s.clone();
            omnigraph::dst_gate::install_gate_hook(Box::new(move || {
                hook_sched
                    .enter(writer)
                    .map(|g| Box::new(g) as Box<dyn std::any::Any + Send>)
            }));
        }

        // Survivors reopen with their own storage view on RecoveryRequired.
        let storage_for_reopen = storage.clone();
        let mut db = Omnigraph::open_with_storage(root, storage)
            .await
            .expect("writer handle on shared root");
        let _finish = sched.as_ref().map(|s| s.finish_on_drop(writer));
        // Start barrier: handles open at each thread's own pace, but the op
        // races begin TOGETHER — without this, thread-start skew lets a fast
        // writer finish before a slow one begins and the universe silently
        // degenerates to sequential. (Blocking the thread is fine: this
        // current_thread runtime runs nothing else.)
        start.wait();
        // Weather starts only once everyone is past the barrier.
        if let Some(f) = &fault {
            f.arm();
        }
        // the arbiter too — a gated call during the OPEN above
        // would deadlock the barrier (WriterFaultStorage's lesson).
        if let Some(s) = &sched {
            s.arm();
        }
        let mut stream = SplitMix64(workload_seed);
        let own_key = format!("cw{writer}");
        let mut claims: Vec<ClaimedWrite> = Vec::with_capacity(ops);
        let mut reopens = 0usize;
        let mut fault_retries = 0usize;
        let mut died = false;

        for op in 0..ops {
            // Disjoint-then-overlapping key ranges (the originally specified shape):
            // op 0 births the writer's private key; the first half stays on
            // it; the second half contends on the shared fixture keys.
            let (query, key) = if op == 0 {
                ("insert_person", own_key.clone())
            } else if op < ops / 2 {
                ("set_age", own_key.clone())
            } else {
                let k = SHARED_KEYS[(stream.next_u64() % SHARED_KEYS.len() as u64) as usize];
                ("set_age", k.to_string())
            };
            let value = encode_value(writer, op);
            let params = mixed_params(&[("$name", key.as_str())], &[("$age", value)]);

            let mut occ_retries = 0usize;
            loop {
                let result = mutate_on(&mut db, "main", MUTATION_QUERIES, query, &params).await;
                // ARM 2, dead-flag-is-authority: the
                // kill wrapper's flag decides death, not the op's result —
                // an op can return Ok when death landed on a best-effort
                // write after its commit point.
                if kill.as_ref().is_some_and(|k| k.dead()) {
                    let outcome = match &result {
                        // Claimed success with storage dead: the claim
                        // stands, the judge verifies it like any other.
                        Ok(_) => ClaimOutcome::Committed,
                        // In flight when death hit: INDEFINITE — may or may
                        // not have applied; the judge allows either.
                        Err(_) => ClaimOutcome::Indeterminate,
                    };
                    claims.push(ClaimedWrite {
                        writer,
                        op,
                        key: key.clone(),
                        value,
                        outcome,
                        occ_retries,
                    });
                    died = true;
                    break;
                }
                match result {
                    Ok(_) => break,
                    Err(err) => {
                        let rendered = format!("{err:?}");
                        // ARM 2 / faults: residue (a peer's death OR an
                        // injected fault mid-op) may trip the recovery
                        // barrier for a LIVE writer. Remedy = the
                        // recovery-barrier prescription (reopen read-write,
                        // then retry). The reopen itself must succeed — a
                        // failing recovery reopen is the failing-reopen class (#473
                        // blast radius) shape.
                        if recovery_legal && rendered.contains("RecoveryRequired") {
                            match Omnigraph::open_with_storage(root, storage_for_reopen.clone())
                                .await
                            {
                                Ok(fresh) => db = fresh,
                                Err(e) => {
                                    let re = format!("{e:?}");
                                    // An injected fault DURING the reopen is
                                    // our own weather, not a failing-reopen-class failure
                                    // — retry the whole op (the barrier will
                                    // re-prescribe the reopen).
                                    if re.contains(crate::harness::FAULT_MARKER) {
                                        fault_retries += 1;
                                        occ_retries += 1;
                                        assert!(
                                            occ_retries < 256,
                                            "writer {writer} op {op}: retry budget \
                                             exhausted reopening under faults"
                                        );
                                        tokio::task::yield_now().await;
                                        continue;
                                    }
                                    panic!(
                                        "writer {writer} op {op}: recovery REOPEN failed \
                                         after RecoveryRequired (failing-reopen shape): {re}"
                                    );
                                }
                            }
                            reopens += 1;
                            occ_retries += 1;
                            assert!(
                                occ_retries < 256,
                                "writer {writer} op {op}: retry budget exhausted on \
                                 {key} across recovery reopens"
                            );
                            tokio::task::yield_now().await;
                            continue;
                        }
                        // FAULTS IN THE RACE: our own injected whole-call
                        // fault — retryable by definition (transient).
                        if rendered.contains(crate::harness::FAULT_MARKER) {
                            fault_retries += 1;
                            occ_retries += 1;
                            assert!(
                                occ_retries < 256,
                                "writer {writer} op {op}: retry budget exhausted on \
                                 {key} under injected faults"
                            );
                            tokio::task::yield_now().await;
                            continue;
                        }
                        // Otherwise: the ONLY legal rejection is an OCC
                        // conflict. Anything else is a finding.
                        assert!(
                            rendered.contains("kind: Conflict"),
                            "writer {writer} op {op} ({query} {key}): illegal rejection \
                             in a concurrent universe: {rendered}"
                        );
                        occ_retries += 1;
                        assert!(
                            occ_retries < 256,
                            "writer {writer} op {op}: OCC retry budget exhausted on \
                             {key} — progress violation under contention"
                        );
                        tokio::task::yield_now().await;
                    }
                }
            }
            if died {
                break; // a dead process issues nothing further
            }
            claims.push(ClaimedWrite {
                writer,
                op,
                key,
                value,
                outcome: ClaimOutcome::Committed,
                occ_retries,
            });
            // Seeded think-time. Without it, the writers serialize WHOLESALE
            // (measured on first contact: alternations == writers-1 in every
            // universe — each writer streaks its whole life; unfair-lock
            // barging shape: the hot writer re-enters the shared write path
            // before any waiter wakes). A short seeded pause releases the
            // path between ops so the interleaving dimension actually gets
            // explored. REAL time — this universe holds no virtual clock.
            let think_us = stream.next_u64() % 500;
            tokio::time::sleep(std::time::Duration::from_micros(think_us)).await;
        }
        // `_finish` leaves the draw domain BEFORE the handle drop, on
        // BOTH exit paths — this explicit drop pair on the normal path,
        // reverse declaration order on a panicking oracle red. (Drop is
        // synchronous and the adapter surface is async, so the drops
        // issue no gated calls.)
        drop(_finish);
        drop(db);
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
        (claims, reopens, fault_retries)
    }))
}

/// ARM 1 — the maintenance actor's life: races Optimize / Cleanup(keep=1) /
/// ensure_indices against the data writers from its own thread + handle.
/// Returns (committed, legal retries, cleanups run). STRICT first-contact
/// error surface: only `kind: Conflict` is legal; anything else panics
/// naming the op — this arm's whole point is learning what a live peer's
/// maintenance actually surfaces.
fn maintenance_life(
    root: &str,
    storage: Arc<dyn StorageAdapter>,
    ops: usize,
    seeds3: (u64, u64, u64),
    start: Arc<std::sync::Barrier>,
    // (arbiter, this actor's scheduler id) when gating is on.
    sched_ctx: Option<(Arc<SeamScheduler>, usize)>,
) -> (usize, usize, usize) {
    let (runtime_seed, ulid_seed, workload_seed) = seeds3;
    let _ = rand::rng().reseed();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("maintenance runtime");
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        if let Some((s, actor)) = &sched_ctx {
            let (hook_sched, hook_actor) = (s.clone(), *actor);
            omnigraph::dst_gate::install_gate_hook(Box::new(move || {
                hook_sched
                    .enter(hook_actor)
                    .map(|g| Box::new(g) as Box<dyn std::any::Any + Send>)
            }));
        }
        let mut db = Omnigraph::open_with_storage(root, storage)
            .await
            .expect("maintenance handle on shared root");
        let _finish = sched_ctx
            .as_ref()
            .map(|(s, actor)| s.finish_on_drop(*actor));
        start.wait();
        if let Some((s, _)) = &sched_ctx {
            s.arm();
        }
        let mut stream = SplitMix64(workload_seed);
        let mut committed = 0usize;
        let mut retries = 0usize;
        let mut cleanups = 0usize;
        for op in 0..ops {
            let kind = stream.next_u64() % 3;
            let mut occ_retries = 0usize;
            loop {
                let result = match kind {
                    0 => Box::pin(db.ensure_indices()).await.map(|_| ()),
                    1 => Box::pin(db.optimize()).await.map(|_| ()),
                    _ => Box::pin(db.cleanup(CleanupPolicyOptions {
                        keep_versions: Some(1),
                        older_than: None,
                    }))
                    .await
                    .map(|_| ()),
                };
                match result {
                    Ok(()) => break,
                    Err(err) => {
                        let rendered = format!("{err:?}");
                        assert!(
                            rendered.contains("kind: Conflict"),
                            "maintenance op {op} (kind {kind}): illegal rejection while \
                             racing live writers: {rendered}"
                        );
                        occ_retries += 1;
                        assert!(
                            occ_retries < 256,
                            "maintenance op {op} (kind {kind}): retry budget exhausted"
                        );
                        tokio::task::yield_now().await;
                    }
                }
            }
            committed += 1;
            retries += occ_retries;
            if kind == 2 {
                cleanups += 1;
            }
            let think_us = stream.next_u64() % 500;
            tokio::time::sleep(std::time::Duration::from_micros(think_us)).await;
        }
        drop(_finish);
        drop(db);
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
        (committed, retries, cleanups)
    }))
}

/// ARM 3 — the branch actor's life: fork→write→merge→delete cycles racing
/// the main writers. Returns (its claim log, merges completed, legal
/// retries). STRICT surface: only `kind: Conflict` is legal on any verb;
/// a merge that fails any other way — including #473's permanent
/// version-collision shape — reds naming the verb (first contact wants to
/// SEE it, carve-outs come after triage).
fn branch_life(
    root: &str,
    storage: Arc<dyn StorageAdapter>,
    actor: usize,
    cycles: usize,
    seeds3: (u64, u64, u64),
    start: Arc<std::sync::Barrier>,
    // the arbiter when gating is on (scheduler id = `actor`).
    sched: Option<Arc<SeamScheduler>>,
) -> (Vec<ClaimedWrite>, usize, usize) {
    let (runtime_seed, ulid_seed, workload_seed) = seeds3;
    let _ = rand::rng().reseed();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("branch actor runtime");
    runtime.block_on(Box::pin(async move {
        omnigraph::dst_ids::install_seeded_ulids(ulid_seed);
        omnigraph::dst_clock::install_logical_clock();
        if let Some(s) = &sched {
            let (hook_sched, hook_actor) = (s.clone(), actor);
            omnigraph::dst_gate::install_gate_hook(Box::new(move || {
                hook_sched
                    .enter(hook_actor)
                    .map(|g| Box::new(g) as Box<dyn std::any::Any + Send>)
            }));
        }
        let mut db = Omnigraph::open_with_storage(root, storage)
            .await
            .expect("branch actor handle on shared root");
        let _finish = sched.as_ref().map(|s| s.finish_on_drop(actor));
        start.wait();
        if let Some(s) = &sched {
            s.arm();
        }
        let mut stream = SplitMix64(workload_seed);
        let mut claims: Vec<ClaimedWrite> = Vec::new();
        let mut merges = 0usize;
        let mut retries = 0usize;

        // One verb attempt with the strict legal set; returns retries spent.
        macro_rules! attempt {
            ($what:expr, $call:expr) => {{
                let mut occ = 0usize;
                loop {
                    match $call.await {
                        Ok(_) => break,
                        Err(err) => {
                            let rendered = format!("{err:?}");
                            assert!(
                                rendered.contains("kind: Conflict"),
                                "branch actor {}: illegal rejection racing live \
                                 writers: {rendered}",
                                $what
                            );
                            occ += 1;
                            assert!(occ < 256, "branch actor {}: retry budget", $what);
                            tokio::task::yield_now().await;
                        }
                    }
                }
                occ
            }};
        }

        for cycle in 0..cycles {
            let branch = format!("cb{cycle}");
            retries += attempt!(
                format!("branch_create {branch}"),
                Box::pin(db.branch_create(&branch))
            );
            for j in 0..BRANCH_WRITES_PER_CYCLE {
                let op = cycle * BRANCH_WRITES_PER_CYCLE + j;
                let key = format!("bw{cycle}x{j}");
                let value = encode_value(actor, op);
                let params = mixed_params(&[("$name", key.as_str())], &[("$age", value)]);
                retries += attempt!(
                    format!("branch write {key}"),
                    mutate_on(&mut db, &branch, MUTATION_QUERIES, "insert_person", &params)
                );
                claims.push(ClaimedWrite {
                    writer: actor,
                    op,
                    key,
                    value,
                    outcome: ClaimOutcome::Committed,
                    occ_retries: 0,
                });
            }
            retries += attempt!(
                format!("branch_merge {branch}"),
                Box::pin(db.branch_merge(&branch, "main"))
            );
            merges += 1;
            retries += attempt!(
                format!("branch_delete {branch}"),
                Box::pin(db.branch_delete(&branch))
            );
            let think_us = stream.next_u64() % 500;
            tokio::time::sleep(std::time::Duration::from_micros(think_us)).await;
        }
        drop(_finish);
        drop(db);
        omnigraph::dst_clock::uninstall_logical_clock();
        omnigraph::dst_ids::uninstall_seeded_ulids();
        (claims, merges, retries)
    }))
}

/// READER ACTOR — live reads during the storm. Every round opens a FRESH
/// read-only handle (bystander style) and reads through BOTH
/// query-channel surfaces. Oracles en route, all red-by-panic:
/// 1. reads NEVER error while writers race (the schema-add class: a read
///    path poisoned by a concurrent structural change);
/// 2. no duplicate person names in the raw scan (the born-on-both class: one
///    logical row, two physical rows);
/// 3. every writer-encoded value decodes to a legal (writer, op) shape;
/// 4. per-key monotonicity: a key owned by one writer must never show an
///    op index moving BACKWARD between successive reads (a later read
///    seeing an earlier value = a time-travel anomaly on the live head).
fn reader_life(
    root: &str,
    storage: Arc<dyn StorageAdapter>,
    reader: usize,
    rounds: usize,
    runtime_seed: u64,
    start: Arc<std::sync::Barrier>,
) -> usize {
    let _ = rand::rng().reseed();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &runtime_seed.to_le_bytes(),
        ))
        .build_local(Default::default())
        .expect("build the reader actor's current-thread runtime");
    runtime.block_on(Box::pin(async move {
        start.wait();
        // last seen (writer, op) per key — monotonicity memory.
        let mut seen: BTreeMap<String, (usize, usize)> = BTreeMap::new();
        let mut completed = 0usize;
        for round in 0..rounds {
            let db = Box::pin(Omnigraph::open_read_only_with_storage(
                root,
                storage.clone(),
            ))
            .await
            .unwrap_or_else(|e| {
                panic!("reader {reader} round {round}: read-only OPEN failed mid-storm: {e:?}")
            });
            // Surface 1: the raw snapshot scan — duplicate detection needs
            // the row list, not the map.
            let rows = person_rows_target(&db, ReadTarget::branch("main")).await;
            for pair in rows.windows(2) {
                assert!(
                    pair[0].0 != pair[1].0,
                    "reader {reader} round {round}: DUPLICATE person rows for \
                     {:?} — one logical row, two physical rows (born-on-both class)",
                    pair[0].0
                );
            }
            // Surface 2: the full query path must not error mid-storm.
            let q = query_main(&db, MUTATION_QUERIES, "all_persons", &Default::default())
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "reader {reader} round {round}: query path ERRORED while \
                         writers race (poisoned-read class): {e:?}"
                    )
                });
            let _ = q.num_rows();
            for (name, age, _) in &rows {
                if let Some((w, op)) = decode_value(*age) {
                    if let Some(&(pw, pop)) = seen.get(name)
                        && pw == w
                    {
                        assert!(
                            op >= pop,
                            "reader {reader} round {round}: {name} moved BACKWARD \
                             in writer {w}'s program order ({pop} -> {op}) — \
                             non-monotone live read"
                        );
                    }
                    seen.insert(name.clone(), (w, op));
                }
            }
            drop(db);
            completed += 1;
            tokio::task::yield_now().await;
        }
        completed
    }))
}

pub fn run_concurrent_universe(root: &str, sc: &ConcurrentScenario) -> ConcurrentReport {
    assert!(
        sc.writers >= 2,
        "a concurrent universe needs at least two writers"
    );
    assert!(sc.ops_per_writer >= 2, "each writer needs at least two ops");
    // Birth certificate. NOTE the envelope: this line does NOT promise replay.
    println!(
        "dst concurrent universe [root={root} seed={} writers={} ops_per_writer={} \
         maintenance_ops={} kill_writer={:?} branch_cycles={}] \
         envelope=bite+oracles-hold (no replay claim)",
        sc.seed,
        sc.writers,
        sc.ops_per_writer,
        sc.maintenance_ops,
        sc.kill_writer,
        sc.branch_cycles
    );

    let mut seeds = SplitMix64(sc.seed);
    let setup_runtime_seed = seeds.next_u64();
    let setup_ulid_seed = seeds.next_u64();
    let entropy_seed = seeds.next_u64();
    let writer_seeds: Vec<(u64, u64, u64)> = (0..sc.writers)
        .map(|_| (seeds.next_u64(), seeds.next_u64(), seeds.next_u64()))
        .collect();
    // Drawn ONLY when the knob is on (see the `maintenance_ops` field doc).
    let maintenance_seeds =
        (sc.maintenance_ops > 0).then(|| (seeds.next_u64(), seeds.next_u64(), seeds.next_u64()));
    let branch_seeds =
        (sc.branch_cycles > 0).then(|| (seeds.next_u64(), seeds.next_u64(), seeds.next_u64()));
    let reader_seeds: Vec<u64> = (0..sc.readers).map(|_| seeds.next_u64()).collect();
    let fault_seeds: Vec<u64> = if sc.writer_fault_pct > 0 {
        (0..sc.writers).map(|_| seeds.next_u64()).collect()
    } else {
        Vec::new()
    };

    crate::harness::clear_process_slots();
    crate::env_knobs::require_pool_env();
    crate::entropy::arm(entropy_seed);

    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("dst-concurrent-universe".into())
            .stack_size(crate::harness::UNIVERSE_STACK_BYTES)
            .spawn_scoped(scope, || {
                let _ = rand::rng().reseed();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .rng_seed(tokio::runtime::RngSeed::from_bytes(
                        &setup_runtime_seed.to_le_bytes(),
                    ))
                    .build_local(Default::default())
                    .expect("setup runtime");

                let storage: Arc<dyn StorageAdapter> = Arc::new(ObjectStorageAdapter::in_memory());

                // ---- setup (sequential, this thread's seams) ----
                omnigraph::dst_ids::install_seeded_ulids(setup_ulid_seed);
                omnigraph::dst_clock::install_logical_clock();
                let (setup_ids, base_map) = runtime.block_on(Box::pin(async {
                    let db = Omnigraph::init_with_storage(
                        root,
                        TEST_SCHEMA,
                        storage.clone(),
                        InitOptions::default(),
                    )
                    .await
                    .expect("init shared root");
                    load_jsonl(&db, TEST_DATA, LoadMode::Overwrite)
                        .await
                        .expect("load fixtures");
                    let ids: BTreeSet<String> = db
                        .list_commits(Some("main"))
                        .await
                        .expect("setup commits")
                        .iter()
                        .map(|c| c.graph_commit_id.clone())
                        .collect();
                    let base =
                        person_map(&person_rows_target(&db, ReadTarget::branch("main")).await);
                    drop(db);
                    (ids, base)
                }));

                // ---- the race: one OS thread per participant ----
                let participants = sc.writers
                    + usize::from(maintenance_seeds.is_some())
                    + usize::from(branch_seeds.is_some())
                    + sc.readers;
                let start = Arc::new(std::sync::Barrier::new(participants));
                // ARM 2: the dying writer gets its own kill wrapper over the
                // shared adapter; peers keep the raw view.
                let kill_storage: Option<Arc<WriterKillStorage>> = sc
                    .kill_writer
                    .map(|(_, k)| Arc::new(WriterKillStorage::new(storage.clone(), k)));
                // Residue that arms recovery can come from a peer's death OR
                // from an injected fault mid-op — either makes a writer's
                // RecoveryRequired legal.
                let recovery_legal = sc.kill_writer.is_some() || sc.writer_fault_pct > 0;
                // FAULTS IN THE RACE: per-writer fault wrappers (the dying
                // writer keeps its kill wrapper only — death semantics stay
                // clean).
                let fault_storages: Vec<Option<Arc<WriterFaultStorage>>> = (0..sc.writers)
                    .map(|w| {
                        let is_dying = sc.kill_writer.is_some_and(|(kw, _)| kw == w);
                        (!is_dying && sc.writer_fault_pct > 0).then(|| {
                            Arc::new(WriterFaultStorage::new(
                                storage.clone(),
                                fault_seeds[w],
                                sc.writer_fault_pct,
                            ))
                        })
                    })
                    .collect();
                // the universe's arbiter. Scheduler ids: writers 0..N,
                // branch actor N (its claim id), maintenance N+1. Readers
                // stay ungated (read-only; initial scope).
                let scheduler: Option<Arc<SeamScheduler>> = sc
                    .seam_schedule
                    .then(|| SeamScheduler::new(sc.seed ^ SEAM_SCHED_SALT));
                if let Some(s) = &scheduler {
                    for w in 0..sc.writers {
                        s.register(w);
                    }
                    if branch_seeds.is_some() {
                        s.register(sc.writers);
                    }
                    if maintenance_seeds.is_some() {
                        s.register(sc.writers + 1);
                    }
                    // Lance-realm gating: the LANCE realm takes turns through
                    // the same arbiter — interpose the provider shim
                    // (idempotent, process-permanent).
                    crate::lance_faults::install();
                    // The park-the-deleter recipe: beneficiary = the branch actor.
                    if sc.park_deleter_hold && branch_seeds.is_some() {
                        s.arm_directed_hold(sc.writers);
                    }
                }
                // Slot set UNCONDITIONALLY (None when the knob is off) — a
                // panicked prior universe must never leak its arbiter into
                // this one (the ACTIVE slot's own first-run lesson).
                crate::lance_faults::set_seam_scheduler(scheduler.clone().map(|s| (s, sc.writers)));
                type BranchStats = (Vec<ClaimedWrite>, usize, usize);
                #[allow(clippy::type_complexity)] // scoped result tuple of the race
                let (results, maintenance_stats, branch_stats, reader_rounds): (
                    Vec<(Vec<ClaimedWrite>, usize, usize)>,
                    (usize, usize, usize),
                    BranchStats,
                    usize,
                ) = std::thread::scope(|writers| {
                    let handles: Vec<_> = writer_seeds
                        .iter()
                        .enumerate()
                        .map(|(w, &seeds3)| {
                            let start = start.clone();
                            let (writer_storage, kill): (
                                Arc<dyn StorageAdapter>,
                                Option<Arc<WriterKillStorage>>,
                            ) = match (&sc.kill_writer, &kill_storage) {
                                (Some((kw, _)), Some(ks)) if *kw == w => {
                                    (ks.clone() as Arc<dyn StorageAdapter>, Some(ks.clone()))
                                }
                                _ => match &fault_storages[w] {
                                    Some(fs) => (fs.clone() as Arc<dyn StorageAdapter>, None),
                                    None => (storage.clone(), None),
                                },
                            };
                            let fault = fault_storages[w].clone();
                            let writer_storage: Arc<dyn StorageAdapter> = match &scheduler {
                                Some(s) => {
                                    Arc::new(ScheduledStorage::new(writer_storage, s.clone(), w))
                                }
                                None => writer_storage,
                            };
                            let sched = scheduler.clone();
                            std::thread::Builder::new()
                                .name(format!("dst-writer-{w}"))
                                .stack_size(crate::harness::UNIVERSE_STACK_BYTES)
                                .spawn_scoped(writers, move || {
                                    writer_life(
                                        root,
                                        writer_storage,
                                        w,
                                        sc.ops_per_writer,
                                        seeds3,
                                        start,
                                        (kill, recovery_legal, fault, sched),
                                    )
                                })
                                .expect("spawn writer thread")
                        })
                        .collect();
                    let maintenance_handle = maintenance_seeds.map(|seeds3| {
                        let maint_actor = sc.writers + 1;
                        let storage: Arc<dyn StorageAdapter> = match &scheduler {
                            Some(s) => Arc::new(ScheduledStorage::new(
                                storage.clone(),
                                s.clone(),
                                maint_actor,
                            )),
                            None => storage.clone(),
                        };
                        let sched_ctx = scheduler.clone().map(|s| (s, maint_actor));
                        let start = start.clone();
                        std::thread::Builder::new()
                            .name("dst-maintenance".into())
                            .stack_size(crate::harness::UNIVERSE_STACK_BYTES)
                            .spawn_scoped(writers, move || {
                                maintenance_life(
                                    root,
                                    storage,
                                    sc.maintenance_ops,
                                    seeds3,
                                    start,
                                    sched_ctx,
                                )
                            })
                            .expect("spawn maintenance thread")
                    });
                    let branch_handle = branch_seeds.map(|seeds3| {
                        let storage: Arc<dyn StorageAdapter> = match &scheduler {
                            Some(s) => Arc::new(ScheduledStorage::new(
                                storage.clone(),
                                s.clone(),
                                sc.writers,
                            )),
                            None => storage.clone(),
                        };
                        let sched = scheduler.clone();
                        let start = start.clone();
                        std::thread::Builder::new()
                            .name("dst-branch-actor".into())
                            .stack_size(crate::harness::UNIVERSE_STACK_BYTES)
                            .spawn_scoped(writers, move || {
                                branch_life(
                                    root,
                                    storage,
                                    sc.writers, // actor id = one past the data writers
                                    sc.branch_cycles,
                                    seeds3,
                                    start,
                                    sched,
                                )
                            })
                            .expect("spawn branch actor thread")
                    });
                    let reader_handles: Vec<_> = reader_seeds
                        .iter()
                        .enumerate()
                        .map(|(r, &rt_seed)| {
                            let storage = storage.clone();
                            let start = start.clone();
                            let rounds = 2 * sc.ops_per_writer;
                            std::thread::Builder::new()
                                .name(format!("dst-reader-{r}"))
                                .stack_size(crate::harness::UNIVERSE_STACK_BYTES)
                                .spawn_scoped(writers, move || {
                                    reader_life(root, storage, r, rounds, rt_seed, start)
                                })
                                .expect("spawn reader thread")
                        })
                        .collect();
                    let mut all = Vec::new();
                    for h in handles {
                        match h.join() {
                            Ok(r) => all.push(r),
                            Err(panic) => std::panic::resume_unwind(panic),
                        }
                    }
                    let m = match maintenance_handle {
                        None => (0, 0, 0),
                        Some(h) => match h.join() {
                            Ok(m) => m,
                            Err(panic) => std::panic::resume_unwind(panic),
                        },
                    };
                    let b = match branch_handle {
                        None => (Vec::new(), 0, 0),
                        Some(h) => match h.join() {
                            Ok(b) => b,
                            Err(panic) => std::panic::resume_unwind(panic),
                        },
                    };
                    let mut r_total = 0usize;
                    for h in reader_handles {
                        match h.join() {
                            Ok(r) => r_total += r,
                            Err(panic) => std::panic::resume_unwind(panic),
                        }
                    }
                    (all, m, b, r_total)
                });
                // The race is over: clear the Lance-realm arbiter slot so
                // the final audit's reads run ungated (and never count as
                // unattributed noise).
                crate::lance_faults::set_seam_scheduler(None);
                let (maintenance_committed, maintenance_retries, maintenance_cleanups) =
                    maintenance_stats;
                let (branch_claims, branch_merges, branch_retries) = branch_stats;
                let branch_committed = branch_claims.len();
                let recovery_reopens: usize = results.iter().map(|(_, r, _)| *r).sum();
                let fault_retries: usize = results.iter().map(|(_, _, f)| *f).sum();
                let writer_faults_injected: usize = fault_storages
                    .iter()
                    .flatten()
                    .map(|fs| fs.injected())
                    .sum();
                let mut claims: Vec<ClaimedWrite> =
                    results.into_iter().flat_map(|(c, _, _)| c).collect();
                // Branch claims join the ONE claim log: ordinary Committed
                // claims, so a merge that drops one reds as LOST UPDATE with
                // no special judge arm.
                claims.extend(branch_claims);

                // ---- final audit (fresh handle, this thread) ----
                let report = runtime.block_on(Box::pin(async {
                    let db = Omnigraph::open_with_storage(root, storage.clone())
                        .await
                        .expect("open the post-race audit handle on the shared root");

                    // OCC commit-id uniqueness + the ascending lineage.
                    let commits = db.list_commits(Some("main")).await.expect("list commits");
                    let mut ids: Vec<String> =
                        commits.iter().map(|c| c.graph_commit_id.clone()).collect();
                    let unique: BTreeSet<&String> = ids.iter().collect();
                    if unique.len() != ids.len() {
                        let mut seen = BTreeSet::new();
                        let dup = ids
                            .iter()
                            .find(|id| !seen.insert(id.as_str()))
                            .expect("count mismatch implies a duplicate");
                        panic!(
                            "OCC invariant: duplicate graph_commit_id {dup} on main \
                             under concurrent writers"
                        );
                    }
                    ids.reverse(); // list_commits is newest-first; we walk oldest-first.

                    // Setup commits must form a prefix of the lineage.
                    let era_start = setup_ids.len();
                    assert!(
                        ids.len() >= era_start
                            && ids[..era_start].iter().all(|id| setup_ids.contains(id)),
                        "setup commits are not a prefix of main's lineage"
                    );

                    // Walk the chain (setup tip + era) attributing by adjacent
                    // history diffs (the history-channel surface). Under a concurrent
                    // Cleanup a PREFIX may be legally unreadable — the live
                    // retention horizon; those claims get the weaker
                    // membership judge. A hole (unreadable AFTER readable) is
                    // always red.
                    let maintenance_active = sc.maintenance_ops > 0;
                    let branch_active = sc.branch_cycles > 0;
                    let mut below_horizon = 0usize;
                    let mut islands = 0usize;
                    let mut maintenance_commit_count = 0usize;
                    let mut attributed: Vec<AttributedCommit> = Vec::new();
                    let mut prev: Option<BTreeMap<String, i64>> = None;
                    let mut s0: Option<BTreeMap<String, i64>> = None;
                    let mut tip_readable = false;
                    for (idx, id) in ids[era_start - 1..].iter().enumerate() {
                        match try_person_map(&db, id).await {
                            Err(e) => {
                                // Readability under a concurrent Cleanup is
                                // NON-CONTIGUOUS by mechanism (a 25,000-seed
                                // concurrent fleet's finding): merges REWRITE the
                                // table's version lineage and each Cleanup
                                // retires ranges between rewrites — readable
                                // ISLANDS, not one prefix. An unreadable
                                // commit anywhere is below-horizon, legal
                                // only when Cleanup can run; attribution
                                // simply cannot span the gap (prev resets).
                                assert!(
                                    maintenance_active,
                                    "commit {id}: unreadable history with no \
                                     maintenance actor in the universe: {e}"
                                );
                                if idx > 0 {
                                    below_horizon += 1;
                                }
                                prev = None; // entering (or continuing) a gap
                                continue;
                            }
                            Ok(map) => {
                                match prev {
                                    None => {
                                        // Island start — no attribution diff
                                        // across the gap.
                                        islands += 1;
                                        if idx == 0 {
                                            tip_readable = true;
                                            assert_eq!(
                                                map, base_map,
                                                "the setup tip no longer renders the \
                                                 fixture base"
                                            );
                                        }
                                        if s0.is_none() {
                                            s0 = Some(map.clone());
                                        }
                                    }
                                    Some(ref p) => {
                                        let mut changes: Vec<(String, i64)> = Vec::new();
                                        for (name, age) in &map {
                                            if p.get(name) != Some(age) {
                                                changes.push((name.clone(), *age));
                                            }
                                        }
                                        let removed: Vec<&String> =
                                            p.keys().filter(|k| !map.contains_key(*k)).collect();
                                        assert!(
                                            removed.is_empty(),
                                            "commit {id}: unexplained person \
                                             deletion(s) {removed:?} in an upsert-only \
                                             universe"
                                        );
                                        if changes.is_empty() {
                                            // Maintenance commits carry no
                                            // writer-encoded person change; a
                                            // recovery pass after a writer death or
                                            // a branch create/delete may legally do
                                            // the same.
                                            assert!(
                                                maintenance_active
                                                    || recovery_legal
                                                    || branch_active,
                                                "commit {id}: empty person-diff with \
                                                 no maintenance actor, no dying \
                                                 writer, and no branch actor — \
                                                 unattributable commit"
                                            );
                                            maintenance_commit_count += 1;
                                        } else {
                                            // Decode every change; ARM 3: a commit
                                            // whose changes ALL belong to the branch
                                            // actor is a MERGE commit folding a whole
                                            // cycle — attribute each write, sorted by
                                            // the actor's program order. Any other
                                            // multi-key commit is a violation.
                                            let mut decoded: Vec<AttributedCommit> = changes
                                                .iter()
                                                .map(|(key, value)| {
                                                    let Some((writer, op)) = decode_value(*value)
                                                    else {
                                                        panic!(
                                                            "commit {id}: value {value} \
                                                             on {key} is not \
                                                             writer-encoded — \
                                                             unattributable write"
                                                        );
                                                    };
                                                    AttributedCommit {
                                                        commit_id: id.clone(),
                                                        writer,
                                                        op,
                                                        key: key.clone(),
                                                        value: *value,
                                                    }
                                                })
                                                .collect();
                                            if decoded.len() > 1 {
                                                assert!(
                                                    branch_active
                                                        && decoded
                                                            .iter()
                                                            .all(|a| a.writer == sc.writers),
                                                    "commit {id}: multi-key commit \
                                                     ({changes:?}) not owned by the \
                                                     branch actor — every data commit \
                                                     writes exactly one key"
                                                );
                                                decoded.sort_by_key(|a| a.op);
                                            }
                                            attributed.append(&mut decoded);
                                        }
                                    }
                                }
                                prev = Some(map);
                            }
                        }
                    }
                    let s0 = s0.expect("at least the head commit must be readable");

                    // Two-channel final audit: full read path vs snapshot scan.
                    let head_rows = person_rows_target(&db, ReadTarget::branch("main")).await;
                    // Born-on-both tripwire: one logical person, two
                    // physical rows (the raw scan sees both; maps collapse).
                    for pair in head_rows.windows(2) {
                        assert!(
                            pair[0].0 != pair[1].0,
                            "final audit: DUPLICATE person rows for {:?} — one \
                             logical row, two physical rows",
                            pair[0].0
                        );
                    }
                    let head = person_map(&head_rows);
                    assert_eq!(
                        prev.expect("at least one readable commit"),
                        head,
                        "history walk's last commit diverges from the live head"
                    );
                    let via_query =
                        query_main(&db, MUTATION_QUERIES, "all_persons", &Default::default())
                            .await
                            .expect("query channel at final audit")
                            .to_rust_json();
                    let mut query_map: BTreeMap<String, i64> = BTreeMap::new();
                    if let serde_json::Value::Array(rows) = via_query {
                        for row in rows {
                            // Projection columns carry the binder prefix:
                            // `return { $p.name, $p.age }` → "p.name"/"p.age".
                            let name = row["p.name"].as_str().unwrap_or_default().to_string();
                            let age = row["p.age"].as_i64().unwrap_or(i64::MIN);
                            query_map.insert(name, age);
                        }
                    }
                    assert_eq!(
                        query_map, head,
                        "query channel disagrees with the snapshot scan at final audit"
                    );

                    // THE judge. Full strength (exact replay + per-claim
                    // lost-update) when the whole chain was readable — one
                    // island. Otherwise: attribution consistency over what
                    // WAS readable + `judge_prefix_membership` aimed at the
                    // FINAL state (island-independent; per-check semantics
                    // on its doc).
                    let _ = &s0; // only the readability expect above uses it
                    let verdict = if below_horizon == 0 && tip_readable {
                        judge_concurrent(&claims, &attributed, &base_map, &head)
                    } else {
                        judge_attribution_consistency(&claims, &attributed)
                            .and_then(|()| judge_prefix_membership(&claims, &head, &base_map))
                    };
                    if let Err(violation) = verdict {
                        panic!("dst concurrent VIOLATION [seed={}]: {violation}", sc.seed);
                    }

                    // The recovery obligation, live form: after the
                    // audit's fresh reopen (which runs recovery), NO residue
                    // may remain — a dead writer's armed sidecar must have
                    // been healed by exactly that reopen.
                    let residue = crate::harness::recovery_residue(&storage, root).await;
                    assert!(
                        residue.is_empty(),
                        "recovery residue survived the audit reopen: {residue:?}"
                    );

                    let alternations = attributed
                        .windows(2)
                        .filter(|p| p[0].writer != p[1].writer)
                        .count();
                    // the arbiter's ledger: grant sequence +
                    // the nondeterminism meter + attribution telemetry.
                    let (grant_log, sched_escapes) =
                        scheduler.as_ref().map(|s| s.snapshot()).unwrap_or_default();
                    let (sched_lance_turns, sched_unattributed) = scheduler
                        .as_ref()
                        .map(|s| s.lance_stats())
                        .unwrap_or_default();
                    let (sched_holds, sched_hold_released, sched_hold_starved) = scheduler
                        .as_ref()
                        .map(|s| s.hold_stats())
                        .unwrap_or_default();
                    ConcurrentReport {
                        sched_turns: grant_log.len(),
                        sched_escapes,
                        grant_log,
                        sched_lance_turns,
                        sched_unattributed,
                        sched_holds,
                        sched_hold_released,
                        sched_hold_starved,
                        committed: claims
                            .iter()
                            .filter(|c| c.outcome == ClaimOutcome::Committed)
                            .count(),
                        occ_retries: claims.iter().map(|c| c.occ_retries).sum(),
                        dead_writer_hit: kill_storage.as_ref().map(|k| k.dead()).unwrap_or(false),
                        dead_writer_label: kill_storage.as_ref().and_then(|k| k.killed_label()),
                        indeterminate: claims
                            .iter()
                            .filter(|c| c.outcome == ClaimOutcome::Indeterminate)
                            .count(),
                        recovery_reopens,
                        maintenance_committed,
                        maintenance_retries,
                        maintenance_cleanups,
                        maintenance_commits: maintenance_commit_count,
                        below_horizon,
                        branch_committed,
                        branch_merges,
                        branch_retries,
                        reader_rounds,
                        writer_faults_injected,
                        fault_retries,
                        islands,
                        alternations,
                        attributed,
                        end_state: head.into_iter().collect(),
                    }
                }));

                omnigraph::dst_clock::uninstall_logical_clock();
                omnigraph::dst_ids::uninstall_seeded_ulids();
                report
            })
            .expect("spawn concurrent universe thread")
            .join()
            .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

// ------------------------------------------------------------------ tests --
// Seeded-blindness proofs for the pure judge: each doctored input must red
// on ITS check (the original verification demands the lost-update one; the
// other four came free from the same reconstruction).
#[cfg(test)]
mod tests {
    use super::*;

    type JudgeFixture = (
        Vec<ClaimedWrite>,
        Vec<AttributedCommit>,
        BTreeMap<String, i64>,
        BTreeMap<String, i64>,
    );

    fn fixture() -> JudgeFixture {
        let base: BTreeMap<String, i64> = [("Alice".to_string(), 30)].into();
        let claims = vec![
            ClaimedWrite {
                writer: 0,
                op: 0,
                key: "cw0".into(),
                value: encode_value(0, 0),
                outcome: ClaimOutcome::Committed,
                occ_retries: 0,
            },
            ClaimedWrite {
                writer: 0,
                op: 1,
                key: "Alice".into(),
                value: encode_value(0, 1),
                outcome: ClaimOutcome::Committed,
                occ_retries: 1,
            },
            ClaimedWrite {
                writer: 1,
                op: 0,
                key: "Alice".into(),
                value: encode_value(1, 0),
                outcome: ClaimOutcome::Committed,
                occ_retries: 0,
            },
        ];
        let attributed = vec![
            AttributedCommit {
                commit_id: "c1".into(),
                writer: 0,
                op: 0,
                key: "cw0".into(),
                value: encode_value(0, 0),
            },
            AttributedCommit {
                commit_id: "c2".into(),
                writer: 1,
                op: 0,
                key: "Alice".into(),
                value: encode_value(1, 0),
            },
            AttributedCommit {
                commit_id: "c3".into(),
                writer: 0,
                op: 1,
                key: "Alice".into(),
                value: encode_value(0, 1),
            },
        ];
        let mut final_map = base.clone();
        for a in &attributed {
            final_map.insert(a.key.clone(), a.value);
        }
        (claims, attributed, base, final_map)
    }

    #[test]
    fn healthy_reconstruction_is_green() {
        let (claims, attributed, base, final_map) = fixture();
        judge_concurrent(&claims, &attributed, &base, &final_map).expect("healthy");
    }

    #[test]
    fn dropped_claim_reds_as_visible_but_unclaimed() {
        let (mut claims, attributed, base, final_map) = fixture();
        claims.retain(|c| !(c.writer == 1 && c.op == 0));
        let err = judge_concurrent(&claims, &attributed, &base, &final_map).unwrap_err();
        assert!(err.contains("VISIBLE-BUT-UNCLAIMED"), "wrong red: {err}");
    }

    #[test]
    fn dropped_commit_reds_as_lost_update() {
        let (claims, mut attributed, base, mut final_map) = fixture();
        // Writer 1's committed op vanishes from history AND the final state —
        // exactly the specified blindness proof: drop one writer's committed op
        // from the merged log, the judge must go red.
        attributed.retain(|a| !(a.writer == 1 && a.op == 0));
        final_map.insert("Alice".into(), encode_value(0, 1));
        let err = judge_concurrent(&claims, &attributed, &base, &final_map).unwrap_err();
        assert!(err.contains("LOST UPDATE"), "wrong red: {err}");
    }

    #[test]
    fn duplicated_commit_reds_as_double_apply() {
        let (claims, mut attributed, base, final_map) = fixture();
        let dup = attributed[1].clone();
        attributed.push(AttributedCommit {
            commit_id: "c4".into(),
            ..dup
        });
        let err = judge_concurrent(&claims, &attributed, &base, &final_map).unwrap_err();
        assert!(err.contains("DOUBLE-APPLY"), "wrong red: {err}");
    }

    #[test]
    fn reordered_writer_ops_red_as_program_order() {
        let (claims, mut attributed, base, final_map) = fixture();
        attributed.swap(0, 2); // writer 0's op 1 now serializes before its op 0
        let err = judge_concurrent(&claims, &attributed, &base, &final_map).unwrap_err();
        assert!(err.contains("PROGRAM ORDER"), "wrong red: {err}");
    }

    #[test]
    fn wrong_final_state_reds_as_final_state() {
        let (claims, attributed, base, mut final_map) = fixture();
        final_map.insert("Alice".into(), 999_999);
        let err = judge_concurrent(&claims, &attributed, &base, &final_map).unwrap_err();
        assert!(err.contains("FINAL STATE"), "wrong red: {err}");
    }

    // ---- arm 1: the prefix-membership judge's own blindness proofs ----

    fn prefix_fixture() -> (
        Vec<ClaimedWrite>,
        BTreeMap<String, i64>,
        BTreeMap<String, i64>,
    ) {
        let base: BTreeMap<String, i64> = [("Alice".to_string(), 30)].into();
        let claim = |writer: usize, op: usize, key: &str| ClaimedWrite {
            writer,
            op,
            key: key.into(),
            value: encode_value(writer, op),
            outcome: ClaimOutcome::Committed,
            occ_retries: 0,
        };
        // w0 births cw0 then writes Alice twice; w1 writes Alice once.
        let claims = vec![
            claim(0, 0, "cw0"),
            claim(0, 1, "Alice"),
            claim(0, 2, "Alice"),
            claim(1, 0, "Alice"),
        ];
        // A legal horizon state: Alice holds w0's LAST write, cw0 exists.
        let s0: BTreeMap<String, i64> = [
            ("Alice".to_string(), encode_value(0, 2)),
            ("cw0".to_string(), encode_value(0, 0)),
        ]
        .into();
        (claims, s0, base)
    }

    #[test]
    fn prefix_healthy_is_green() {
        let (claims, s0, base) = prefix_fixture();
        judge_prefix_membership(&claims, &s0, &base).expect("healthy horizon");
    }

    #[test]
    fn prefix_phantom_value_is_red() {
        let (claims, mut s0, base) = prefix_fixture();
        s0.insert("Alice".into(), encode_value(3, 7));
        let err = judge_prefix_membership(&claims, &s0, &base).unwrap_err();
        assert!(err.contains("PREFIX PHANTOM"), "wrong red: {err}");
    }

    #[test]
    fn prefix_lost_key_is_red() {
        let (claims, mut s0, base) = prefix_fixture();
        s0.remove("cw0");
        let err = judge_prefix_membership(&claims, &s0, &base).unwrap_err();
        assert!(err.contains("PREFIX LOST KEY"), "wrong red: {err}");
    }

    #[test]
    fn prefix_lost_update_is_red() {
        let (claims, mut s0, base) = prefix_fixture();
        s0.insert("Alice".into(), 30); // base value despite committed writes
        let err = judge_prefix_membership(&claims, &s0, &base).unwrap_err();
        assert!(err.contains("PREFIX LOST UPDATE"), "wrong red: {err}");
    }

    // ---- arm 2: indeterminate (indefinite-error) handling ----

    #[test]
    fn indeterminate_claim_may_be_absent() {
        let (mut claims, mut attributed, base, mut final_map) = fixture();
        // Writer 1 died mid-op: its Alice write becomes Indeterminate and
        // never applied — history and final state agree it is absent.
        claims.iter_mut().find(|c| c.writer == 1).unwrap().outcome = ClaimOutcome::Indeterminate;
        attributed.retain(|a| a.writer != 1);
        final_map.insert("Alice".into(), encode_value(0, 1));
        judge_concurrent(&claims, &attributed, &base, &final_map)
            .expect("absent indeterminate is legal");
    }

    #[test]
    fn indeterminate_claim_may_be_present() {
        let (mut claims, attributed, base, final_map) = fixture();
        // Same death, but the dying op DID land before the commit point —
        // present exactly once is equally legal.
        claims.iter_mut().find(|c| c.writer == 1).unwrap().outcome = ClaimOutcome::Indeterminate;
        judge_concurrent(&claims, &attributed, &base, &final_map)
            .expect("applied indeterminate is legal");
    }

    #[test]
    fn prefix_indeterminate_only_key_may_be_absent() {
        let (mut claims, mut s0, base) = prefix_fixture();
        // cw0's only write becomes the dying op; the key never appearing at
        // the horizon is then legal (indefinite), unlike a committed birth.
        claims.iter_mut().find(|c| c.key == "cw0").unwrap().outcome = ClaimOutcome::Indeterminate;
        s0.remove("cw0");
        judge_prefix_membership(&claims, &s0, &base)
            .expect("indeterminate-only key absence is legal");
    }

    #[test]
    fn prefix_intermediate_survivor_is_red() {
        let (claims, mut s0, base) = prefix_fixture();
        // w0's op 1 on Alice is superseded by its op 2 in program order —
        // an intermediate value surviving to the horizon is an ordering
        // violation no serialization can produce.
        s0.insert("Alice".into(), encode_value(0, 1));
        let err = judge_prefix_membership(&claims, &s0, &base).unwrap_err();
        assert!(err.contains("PREFIX ORDER"), "wrong red: {err}");
    }

    // ---- the arbiter's own determinism + escape proofs ----

    #[test]
    fn seam_scheduler_draw_sequence_is_seed_deterministic() {
        // Same seed + same live set ⇒ same draw sequence (the induction's
        // base ingredient); a different seed must move it (non-vacuity —
        // the arbiter is seed-driven, not arrival-driven).
        let seq = |seed: u64| -> Vec<usize> {
            let s = SeamScheduler::new(seed);
            for a in 0..3 {
                s.register(a);
            }
            let mut st = s.state.lock().unwrap();
            (0..32)
                .map(|_| SeamScheduler::draw(&mut st).expect("nonempty live set"))
                .collect()
        };
        assert_eq!(seq(7), seq(7));
        assert_ne!(seq(7), seq(8));
    }

    #[test]
    fn seam_scheduler_escape_is_counted_when_the_drawn_actor_never_arrives() {
        // Only a stalled actor is drawable: every draw names actor 1, which
        // never presents a call — the arbiter must ESCAPE (re-draw over the
        // arrival set) rather than hang, and must COUNT it: the strict
        // replay claim dies with the first escape, never silently.
        let s = SeamScheduler::new(11);
        s.register(1);
        s.arm();
        let g = s.enter(0).expect("armed scheduler gates the call");
        drop(g);
        let (log, escapes) = s.snapshot();
        assert_eq!(log, vec![0], "the pending actor was granted via escape");
        assert!(escapes >= 1, "the bypass was not counted");
    }

    #[test]
    fn seam_scheduler_is_a_no_op_until_armed() {
        // Before the start barrier arms it, enter() must not gate anything
        // (a gated call during a handle OPEN deadlocks the barrier).
        let s = SeamScheduler::new(11);
        s.register(0);
        assert!(s.enter(0).is_none(), "unarmed scheduler must not gate");
        let (log, escapes) = s.snapshot();
        assert!(log.is_empty() && escapes == 0);
    }
}
