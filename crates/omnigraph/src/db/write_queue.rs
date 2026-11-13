//! Per-`(table_key, branch)` writer queues.
//!
//! These queues are the engine's process-local, root-scoped write-serialization
//! mechanism. The server normally holds one lockless `Arc<Omnigraph>`, but
//! independently opened handles for the same canonical local root identity
//! (or the same opaque object-store URI) share this manager too. Legacy
//! writers serialize only on `(table_key, branch_ref)` so
//! disjoint keys can proceed concurrently. RFC-022-enrolled mutation/load
//! attempts additionally take a coarse branch effect gate because validation
//! may depend on tables they do not write. This module owns both queue classes;
//! callers in `MutationStaging::commit_all`, branch controls, `branch_merge`,
//! `schema_apply`, `ensure_indices`, cleanup, and branch forking acquire the
//! applicable guards before a table effect or destructive ref action.
//! Serialization remains in-process only; cross-process writers on one graph
//! remain one-winner-CAS at publish.
//!
//! ## Lock shapes: exclusive `tokio::sync::Mutex<()>` per key, one
//! shared/exclusive schema gate
//!
//! Every writer stages detached from its table's pin and becomes visible
//! only through the manifest CAS (RFC 0067), so the queue is not a
//! correctness authority for table data. Its application-layer job is to
//! keep same-process writers on one `(table_key, branch_ref)` from
//! interleaving between revalidation and publication (the loser's detached
//! versions would be wasted garbage) and to serialize destructive ref
//! deletion and fork creation against live writers. Table and branch keys
//! stay exclusive. The graph-global schema gate is the one
//! shared/exclusive slot ([`SchemaGateSlot`]): a pass that only READS the
//! accepted contract/catalog view takes a shared permit, and only a pass
//! that can CHANGE which view is accepted (schema apply, the system-column
//! upgrade, and the contract install/discard/reload passes) takes the
//! exclusive side — the classification rule and its safety argument are
//! RFC 2026-09-18-shared-schema-gate. Under the old protocol every writer
//! took this gate exclusively because a mutation could advance a Lance
//! HEAD before discovering an in-flight apply; detached staging abolished
//! that failure, leaving the gate as pure serialization.
//!
//! ## Sorted-order acquisition
//!
//! `acquire_many` accepts a slice of keys and acquires them in
//! lexicographic order. Multi-table writers and control paths (mutation
//! finalize, branch merge, schema apply, and maintenance) MUST go through
//! `acquire_many` so all callers agree on acquisition order — this is
//! how lock-order inversion deadlock is prevented.
//!
//! ## Gates
//!
//! Every place where one operation waits for another in this process is one
//! of six gates, and this module is where they live: `table` (a
//! [`QueueSlot`] per `(table_key, branch)`), `branch` (a slot per branch),
//! `schema_shared` and `schema_exclusive` (the two sides of
//! [`SchemaGateSlot`]), `export` (the export cut against destructive
//! controls) and `coordinator_cas` (the handle's coordinator pointer swap,
//! `Omnigraph::install_coordinator`). A gate acquisition crosses three
//! decision seams, `gate.<name>.acquire` on entering the wait,
//! `gate.<name>.hold` once the primitive is owned and `gate.<name>.release`
//! when the guard drops, each outside any arbiter turn and each an
//! observation site: the decision is read and passed, so an installed
//! observer (GQT measure, the trace, a DST probe) sees every wait without the
//! engine changing behavior. The `export` gate never waits: its two `try_*`
//! acquisitions refuse instead, so it crosses `hold` and `release` only. A
//! lost coordinator CAS round is a wait of the CAS kind: every round crosses
//! `acquire`, the winning swap `hold`, and a refused install `refused`, the
//! one `acquire` that ends with no hold. Not gates, and so outside this
//! module: `Omnigraph::feed_cut_cache` and `Omnigraph::merge_authority_cache`,
//! caches held across no round trip, and the operator-local locks under
//! `engine/`, intra-query state. The `forbidden_apis` guard refuses a new
//! blocking primitive elsewhere in `db/` and `exec/` without a sentinel
//! saying why it is not a gate.
//!
//! ## Preparation takes no gate
//!
//! The gates are not reentrant, and a write sent back to prepare again holds
//! its branch's writer gates from that preparation through its publication
//! ([`crate::db::reprepare`]). So a writer gate taken while a write prepares
//! would wait on the attempt's own gate forever. Each attempt of a mutation,
//! a load or a Blob write therefore runs inside a preparation scope
//! ([`preparing`]), which its commit point ends ([`end_preparation`]) just
//! before it takes the gates. Every acquisition below refuses inside the
//! scope with an internal error, so a path that breaks the rule fails loudly
//! in the first test that reaches it, on a first attempt as on a gated one,
//! instead of hanging. The scope is a task-local: a task that preparation
//! spawns does not inherit it.

use std::cell::Cell;
use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard,
    RwLock as AsyncRwLock,
};

use crate::error::{OmniError, Result};
use crate::seams::{DecideSeam, decide_seam};

tokio::task_local! {
    /// Whether the current task is preparing an attempt of a write: set by
    /// [`preparing`], cleared by [`end_preparation`] at the attempt's commit
    /// point.
    static PREPARING: Cell<bool>;
}

/// Run `attempt`, one attempt of a write, as a preparation: until the
/// attempt reaches its commit point ([`end_preparation`]), every writer-gate
/// acquisition in this task refuses.
pub(crate) async fn preparing<F: Future>(attempt: F) -> F::Output {
    PREPARING.scope(Cell::new(true), attempt).await
}

/// The attempt reached its commit point: it takes the writer gates from here
/// on. Returns whether it was preparing, that is whether the attempt ran
/// inside [`preparing`] and had not ended its preparation already.
pub(crate) fn end_preparation() -> bool {
    PREPARING
        .try_with(|preparing| preparing.replace(false))
        .unwrap_or(false)
}

/// Whether the current task is preparing an attempt of a write.
pub(crate) fn is_preparing() -> bool {
    PREPARING.try_with(Cell::get).unwrap_or(false)
}

/// Refuse a writer-gate acquisition inside a preparation: the attempt may
/// hold its branch's gates, and the gates are not reentrant.
fn refuse_while_preparing(gate: &'static str) -> Result<()> {
    if is_preparing() {
        return Err(OmniError::manifest_internal(format!(
            "a write took the {gate} gate while preparing an attempt; preparation \
             must take no writer gate, since a write sent back prepares again \
             holding its branch's gates"
        )));
    }
    Ok(())
}

/// DST seam: one write-queue slot — the lock plus its RELEASE EPOCH.
/// The epoch closes an arrival-order leak: without it the mutex RELEASE
/// is an ungated event, so a contender's retry races the holder's guard
/// drop and same-seed runs diverge. With the epoch, releases are
/// TURN-ORDERED (see `QueueGuard`) and a contender re-attempts only
/// after observing a release — ordered strictly after the unlock, never
/// racing it.
pub(crate) struct QueueSlot {
    lock: Arc<AsyncMutex<()>>,
    releases: std::sync::atomic::AtomicU64,
    gate: &'static GateSeams,
}

impl QueueSlot {
    fn new(gate: &'static GateSeams) -> Arc<Self> {
        Arc::new(Self {
            lock: Arc::new(AsyncMutex::new(())),
            releases: std::sync::atomic::AtomicU64::new(0),
            gate,
        })
    }
}

/// The three seams one gate crosses (module doc §Gates).
pub(crate) struct GateSeams {
    acquire: &'static DecideSeam,
    hold: &'static DecideSeam,
    release: &'static DecideSeam,
}

decide_seam! {
    /// A writer enters the wait for its `(table_key, branch)` slot.
    pub static GATE_TABLE_ACQUIRE = ("gate.table.acquire", Unreachable, [Fail]);
}
decide_seam! {
    /// The `(table_key, branch)` slot is owned.
    pub static GATE_TABLE_HOLD = ("gate.table.hold", Unreachable, [Fail]);
}
decide_seam! {
    /// The `(table_key, branch)` slot's guard drops.
    pub static GATE_TABLE_RELEASE = ("gate.table.release", Unreachable, [Fail]);
}
decide_seam! {
    /// A writer enters the wait for its branch's effect gate.
    pub static GATE_BRANCH_ACQUIRE = ("gate.branch.acquire", Unreachable, [Fail]);
}
decide_seam! {
    /// The branch effect gate is owned.
    pub static GATE_BRANCH_HOLD = ("gate.branch.hold", Unreachable, [Fail]);
}
decide_seam! {
    /// The branch effect gate's guard drops.
    pub static GATE_BRANCH_RELEASE = ("gate.branch.release", Unreachable, [Fail]);
}
decide_seam! {
    /// A reader of the accepted schema view enters the wait for the shared
    /// side of the schema gate.
    pub static GATE_SCHEMA_SHARED_ACQUIRE = ("gate.schema_shared.acquire", Unreachable, [Fail]);
}
decide_seam! {
    /// The shared schema permit is owned.
    pub static GATE_SCHEMA_SHARED_HOLD = ("gate.schema_shared.hold", Unreachable, [Fail]);
}
decide_seam! {
    /// The shared schema permit drops.
    pub static GATE_SCHEMA_SHARED_RELEASE = ("gate.schema_shared.release", Unreachable, [Fail]);
}
decide_seam! {
    /// A pass that changes the accepted schema view enters the wait for the
    /// exclusive side of the schema gate.
    pub static GATE_SCHEMA_EXCLUSIVE_ACQUIRE = ("gate.schema_exclusive.acquire", Unreachable, [Fail]);
}
decide_seam! {
    /// The exclusive schema permit is owned.
    pub static GATE_SCHEMA_EXCLUSIVE_HOLD = ("gate.schema_exclusive.hold", Unreachable, [Fail]);
}
decide_seam! {
    /// The exclusive schema permit drops.
    pub static GATE_SCHEMA_EXCLUSIVE_RELEASE = ("gate.schema_exclusive.release", Unreachable, [Fail]);
}
decide_seam! {
    /// An export cut or a destructive control owns its side of the export
    /// gate; the gate refuses instead of waiting, so it has no `acquire`.
    pub static GATE_EXPORT_HOLD = ("gate.export.hold", Unreachable, [Fail]);
}
decide_seam! {
    /// An export cut or destructive control permit drops.
    pub static GATE_EXPORT_RELEASE = ("gate.export.release", Unreachable, [Fail]);
}

static TABLE_GATE: GateSeams = GateSeams {
    acquire: &GATE_TABLE_ACQUIRE,
    hold: &GATE_TABLE_HOLD,
    release: &GATE_TABLE_RELEASE,
};

static BRANCH_GATE: GateSeams = GateSeams {
    acquire: &GATE_BRANCH_ACQUIRE,
    hold: &GATE_BRANCH_HOLD,
    release: &GATE_BRANCH_RELEASE,
};

static SCHEMA_SHARED_GATE: GateSeams = GateSeams {
    acquire: &GATE_SCHEMA_SHARED_ACQUIRE,
    hold: &GATE_SCHEMA_SHARED_HOLD,
    release: &GATE_SCHEMA_SHARED_RELEASE,
};

static SCHEMA_EXCLUSIVE_GATE: GateSeams = GateSeams {
    acquire: &GATE_SCHEMA_EXCLUSIVE_ACQUIRE,
    hold: &GATE_SCHEMA_EXCLUSIVE_HOLD,
    release: &GATE_SCHEMA_EXCLUSIVE_RELEASE,
};

/// A held write-queue lock whose RELEASE is visible to the harness arbiter:
/// dropping it takes a turn (when the gate hook is installed), unlocks
/// INSIDE that turn, and bumps the slot's release epoch — so an unlock is a
/// scheduled event with a deterministic position in the grant sequence,
/// exactly like the acquisition attempts. Uninstalled: a plain unlock.
pub(crate) struct QueueGuard {
    inner: Option<OwnedMutexGuard<()>>,
    slot: Arc<QueueSlot>,
}

impl Drop for QueueGuard {
    fn drop(&mut self) {
        observed(self.slot.gate.release);
        // Turn first (None when uninstalled/unarmed — plain path), unlock
        // under it, bump the epoch last: a waiter that observes the bump
        // sees a lock that is genuinely free.
        let _turn = crate::dst_gate::turn();
        drop(self.inner.take());
        self.slot
            .releases
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Scheduled lock acquisition. Uninstalled (or hook declining): plain
/// blocking `lock_owned`. Installed: the contender STAYS IN THE TURN LOOP
/// the whole time — always visible and drawable at the arbiter (a
/// turn-free wait was measured to reproduce the invisibility disease as
/// systematic escapes) — but a granted turn ATTEMPTS the lock only when
/// the slot's release epoch has advanced past the last failed attempt
/// (epoch read UNDER the turn, so it is turn-ordered); otherwise the turn
/// is a deterministic no-op. Attempts therefore never race the unlock
/// (releases bump the epoch inside their own turns, see `QueueGuard`),
/// and every event — attempt, no-op, release — has a seeded position in
/// the grant sequence.
async fn scheduled_lock(slot: Arc<QueueSlot>) -> QueueGuard {
    observed(slot.gate.acquire);
    let inner = scheduled_lock_wait(&slot).await;
    observed(slot.gate.hold);
    QueueGuard {
        inner: Some(inner),
        slot,
    }
}

async fn scheduled_lock_wait(slot: &QueueSlot) -> OwnedMutexGuard<()> {
    let mut wait_epoch: Option<u64> = None;
    loop {
        match crate::dst_gate::turn() {
            None => return Arc::clone(&slot.lock).lock_owned().await,
            Some(_turn) => {
                let epoch_now = slot.releases.load(std::sync::atomic::Ordering::SeqCst);
                if wait_epoch.is_none_or(|e| epoch_now != e) {
                    match Arc::clone(&slot.lock).try_lock_owned() {
                        Ok(guard) => return guard,
                        Err(_) => wait_epoch = Some(epoch_now),
                    }
                }
                // else: no release since the failed attempt — a no-op turn.
            }
        }
        tokio::task::yield_now().await;
    }
}

/// Queue key: `(table_key, branch_ref)`. `branch_ref = None` means main.
///
/// Branch is part of the key because the same Lance dataset can be
/// pinned at different versions on different branches; concurrent
/// writes to the same `table_key` on disjoint branches must NOT
/// serialize at the queue.
pub(crate) type TableQueueKey = (String, Option<String>);

/// The graph-global schema gate: the one shared/exclusive slot.
///
/// It serializes every graph-global schema writer (schema apply and the
/// system-column upgrade), every pass that installs, discards, or
/// republishes the accepted schema-contract view, and every pass that
/// changes the live branch-ref set without a CAS over it (branch create
/// and create-from, whose namespace inventory precedes the native create)
/// against each other — exclusively — while readers of the accepted view
/// (ordinary writers, merges, maintenance, branch delete, read-view
/// captures) share.
///
/// The gate is non-reentrant in BOTH modes on one task: an exclusive
/// holder re-acquiring either side self-deadlocks exactly like the
/// per-key mutexes, and a shared holder re-acquiring the shared side can
/// park forever behind a queued writer (tokio's `RwLock` is
/// write-preferring in plain mode). Callers therefore never take the gate
/// twice on one call path; `refresh_coordinator_only` exists for exactly
/// this reason.
///
/// Fairness is asymmetric by mode: PLAIN mode inherits tokio's
/// write-preferring FIFO (once an exclusive acquisition is queued, later
/// shared permits park behind it, so writers cannot starve a schema
/// apply); INSTALLED (DST) mode never enters the native waiter queue —
/// both sides use `try_*` inside the turn loop, so grant order and
/// starvation-freedom are properties of the seed, asserted only by the
/// plain-mode unit test.
///
/// The release epoch plays the same role as [`QueueSlot::releases`]: any
/// permit drop (shared or exclusive) takes a turn, releases inside it,
/// and bumps the epoch last, so contenders re-attempt only after a
/// turn-ordered release and same-seed runs cannot diverge on the unlock.
#[derive(Default)]
pub(crate) struct SchemaGateSlot {
    lock: Arc<AsyncRwLock<()>>,
    releases: std::sync::atomic::AtomicU64,
}

impl SchemaGateSlot {
    /// The release protocol shared by both permits, the same as
    /// `QueueGuard`'s: turn first, release inside it, bump the epoch last so
    /// an observed bump implies a genuinely released slot.
    fn release<G>(&self, guard: Option<G>, gate: &GateSeams) {
        observed(gate.release);
        let _turn = crate::dst_gate::turn();
        drop(guard);
        self.releases
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// A shared schema permit: proof that no contract-lifecycle pass is
/// concurrently swapping the accepted schema/catalog view. Held by
/// readers of that view for the duration of their gate-ordered work
/// (writers: through manifest publish).
#[must_use = "dropping the permit releases the shared schema gate"]
pub(crate) struct SchemaSharedPermit {
    inner: Option<OwnedRwLockReadGuard<()>>,
    slot: Arc<SchemaGateSlot>,
}

/// An exclusive schema permit: sole ownership of the accepted-view
/// transition. Held by schema apply, the system-column upgrade, the
/// contract install/discard/reload passes, and branch create.
#[must_use = "dropping the permit releases the exclusive schema gate"]
pub(crate) struct SchemaExclusivePermit {
    inner: Option<OwnedRwLockWriteGuard<()>>,
    slot: Arc<SchemaGateSlot>,
}

impl Drop for SchemaSharedPermit {
    fn drop(&mut self) {
        self.slot.release(self.inner.take(), &SCHEMA_SHARED_GATE);
    }
}

impl Drop for SchemaExclusivePermit {
    fn drop(&mut self) {
        self.slot.release(self.inner.take(), &SCHEMA_EXCLUSIVE_GATE);
    }
}

decide_seam! {
    /// An exclusive acquisition whose first attempt found the gate held, so
    /// the caller is about to queue behind the holder; crossed once per
    /// acquisition, outside any turn. An observation site: the acquire has
    /// no outcome to give a fired effect, so a test arms `observe` or `hold`
    /// here to see a queued writer instead of timing its absence.
    pub static SCHEMA_GATE_EXCLUSIVE_QUEUED = ("write_queue.schema_gate_exclusive_queued", Unreachable, [Fail]);
}

/// Cross an observation seam; the decision is read and passed.
pub(crate) fn observed(seam: &'static DecideSeam) {
    #[cfg(feature = "failpoints")]
    let _ = seam.crossed();
    #[cfg(not(feature = "failpoints"))]
    let _ = seam;
}

/// One side of the schema gate on the [`scheduled_lock`] protocol: plain
/// blocking acquire uninstalled; installed, try only when the release epoch
/// moved, yielding every turn. `queued` is crossed once the first try fails.
async fn scheduled_schema_permit<G>(
    slot: &SchemaGateSlot,
    gate: &GateSeams,
    acquire: impl AsyncFnOnce(Arc<AsyncRwLock<()>>) -> G,
    try_acquire: impl Fn(Arc<AsyncRwLock<()>>) -> Option<G>,
    queued: Option<&'static DecideSeam>,
) -> G {
    observed(gate.acquire);
    let permit = scheduled_schema_wait(slot, acquire, try_acquire, queued).await;
    observed(gate.hold);
    permit
}

async fn scheduled_schema_wait<G>(
    slot: &SchemaGateSlot,
    acquire: impl AsyncFnOnce(Arc<AsyncRwLock<()>>) -> G,
    try_acquire: impl Fn(Arc<AsyncRwLock<()>>) -> Option<G>,
    mut queued: Option<&'static DecideSeam>,
) -> G {
    let mut wait_epoch: Option<u64> = None;
    loop {
        match crate::dst_gate::turn() {
            None => {
                if let Some(guard) = try_acquire(Arc::clone(&slot.lock)) {
                    return guard;
                }
                if let Some(seam) = queued.take() {
                    observed(seam);
                }
                return acquire(Arc::clone(&slot.lock)).await;
            }
            Some(_turn) => {
                let epoch_now = slot.releases.load(std::sync::atomic::Ordering::SeqCst);
                if wait_epoch.is_none_or(|e| epoch_now != e) {
                    match try_acquire(Arc::clone(&slot.lock)) {
                        Some(guard) => return guard,
                        None => wait_epoch = Some(epoch_now),
                    }
                }
                // else: no release since the failed attempt — a no-op turn.
            }
        }
        if wait_epoch.is_some()
            && let Some(seam) = queued.take()
        {
            observed(seam);
        }
        tokio::task::yield_now().await;
    }
}

async fn scheduled_schema_shared(slot: Arc<SchemaGateSlot>) -> SchemaSharedPermit {
    let guard = scheduled_schema_permit(
        &slot,
        &SCHEMA_SHARED_GATE,
        async |lock| lock.read_owned().await,
        |lock| lock.try_read_owned().ok(),
        None,
    )
    .await;
    SchemaSharedPermit {
        inner: Some(guard),
        slot,
    }
}

async fn scheduled_schema_exclusive(slot: Arc<SchemaGateSlot>) -> SchemaExclusivePermit {
    let guard = scheduled_schema_permit(
        &slot,
        &SCHEMA_EXCLUSIVE_GATE,
        async |lock| lock.write_owned().await,
        |lock| lock.try_write_owned().ok(),
        Some(&SCHEMA_GATE_EXCLUSIVE_QUEUED),
    )
    .await;
    SchemaExclusivePermit {
        inner: Some(guard),
        slot,
    }
}

/// Ordered write-gate envelope for an RFC-022 effect writer: shared
/// schema permit, then the branch gate, then the lex-sorted table gates,
/// held from revalidation through manifest publish.
///
/// Field order is drop order — schema releases first, then branch, then
/// tables — preserving the exact release sequence the former homogeneous
/// guard Vec produced, so seeded DST grant sequences keep their event
/// order at release.
#[must_use = "dropping the gates releases the write envelope"]
pub(crate) struct HeldWriteGates {
    _schema: SchemaSharedPermit,
    _branch: QueueGuard,
    _tables: Vec<QueueGuard>,
}

impl HeldWriteGates {
    /// Assemble the envelope from the gates in acquisition order: the
    /// shared schema permit, then the branch gate, then the sorted table
    /// gates.
    pub(crate) fn new(
        schema: SchemaSharedPermit,
        branch: QueueGuard,
        tables: Vec<QueueGuard>,
    ) -> Self {
        Self {
            _schema: schema,
            _branch: branch,
            _tables: tables,
        }
    }
}

/// The first two gates of a write's envelope, the shared schema permit and
/// one branch's gate, taken before the write prepares. A write sent back to
/// prepare again holds them from its next capture through its publication
/// ([`crate::db::reprepare::PreEffectReprepares`]), so no publisher in the
/// process can move the branch under that attempt; `commit_all` adds the
/// table gates in the usual order.
#[must_use = "dropping the gates releases the branch"]
pub(crate) struct BranchWriteGates {
    branch_key: Option<String>,
    schema: SchemaSharedPermit,
    branch: QueueGuard,
}

impl BranchWriteGates {
    /// The branch these gates hold, `None` for main.
    pub(crate) fn branch_key(&self) -> Option<&str> {
        self.branch_key.as_deref()
    }

    /// Complete the envelope with `tables`, taken after the branch gate.
    pub(crate) fn with_tables(self, tables: Vec<QueueGuard>) -> HeldWriteGates {
        HeldWriteGates::new(self.schema, self.branch, tables)
    }
}

/// Non-cloneable ownership of the sole immutable export cut for one graph.
///
/// The root registry stores weak references, so retaining the manager here is
/// part of the exclusion contract: a cut can outlive the `Omnigraph` handle
/// that captured it without a reopened handle creating an independent gate.
#[must_use = "dropping the permit releases the export cut"]
pub(crate) struct ExportCutPermit {
    _manager: Arc<WriteQueueManager>,
    _permit: OwnedRwLockWriteGuard<()>,
}

/// Shared exclusion held by controls that can remove or reuse an export cut's
/// exact path/version coordinates.
#[must_use = "dropping the permit releases export destructive control"]
pub(crate) struct ExportDestructivePermit {
    _manager: Arc<WriteQueueManager>,
    _permit: OwnedRwLockReadGuard<()>,
}

impl Drop for ExportCutPermit {
    fn drop(&mut self) {
        observed(&GATE_EXPORT_RELEASE);
    }
}

impl Drop for ExportDestructivePermit {
    fn drop(&mut self) {
        observed(&GATE_EXPORT_RELEASE);
    }
}

/// Per-`(table_key, branch)` writer queue manager.
///
/// Every `Omnigraph` handle for one canonical root identity shares the same
/// manager via a process-global weak registry. This matters beyond HTTP's usual
/// `Arc<Omnigraph>` shape: a separately-opened handle can apply a schema
/// or delete refs, which must serialize with a live writer
/// owned by the first handle. The registry deliberately keys only by the queue root
/// identity; custom storage adapters for the same URI conservatively serialize
/// too.
#[derive(Default)]
pub(crate) struct WriteQueueManager {
    /// Held only briefly per `acquire` call: clone out the per-key Arc,
    /// release the std mutex, then await the per-key tokio Mutex.
    queues: Mutex<HashMap<TableQueueKey, Arc<QueueSlot>>>,
    /// Coarse per-branch effect gate used by RFC-022 writers.
    ///
    /// This is deliberately separate from `queues`: a branch is authority,
    /// not a synthetic table key. Registered graph-visible effect writers
    /// acquire this gate before any table queue and hold it through manifest
    /// publication; explicit authority/physical exceptions follow their own
    /// registered ordering contracts.
    branch_queues: Mutex<HashMap<Option<String>, Arc<QueueSlot>>>,
    /// One immutable export owns the write side through output. Cooperative
    /// destructive controls share the read side, so they remain mutually
    /// concurrent but cannot remove a path/version beneath a live cut.
    export_gate: Arc<AsyncRwLock<()>>,
    /// The graph-global shared/exclusive schema gate; see
    /// [`SchemaGateSlot`].
    schema_gate: Arc<SchemaGateSlot>,
}

impl WriteQueueManager {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Return the process-wide queue manager for one canonical graph-root
    /// identity.
    /// Weak values avoid retaining every graph URI ever opened by a long-lived
    /// multi-tenant process; lookup opportunistically removes dead entries.
    pub(crate) fn for_root(root_identity: &str) -> Arc<Self> {
        static REGISTRY: OnceLock<Mutex<HashMap<String, Weak<WriteQueueManager>>>> =
            OnceLock::new();
        let registry = REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
        let mut roots = registry.lock().expect("root write queue registry poisoned");
        if let Some(existing) = roots.get(root_identity).and_then(Weak::upgrade) {
            return existing;
        }
        roots.retain(|_, manager| manager.strong_count() > 0);
        let manager = Arc::new(Self::new());
        roots.insert(root_identity.to_string(), Arc::downgrade(&manager));
        manager
    }

    /// Get-or-create the per-key queue and clone its Arc.
    fn slot(&self, key: &TableQueueKey) -> Arc<QueueSlot> {
        let mut map = self.queues.lock().expect("write queue map poisoned");
        if let Some(existing) = map.get(key) {
            return Arc::clone(existing);
        }
        let fresh = QueueSlot::new(&TABLE_GATE);
        map.insert(key.clone(), Arc::clone(&fresh));
        fresh
    }

    fn branch_slot(&self, branch: &Option<String>) -> Arc<QueueSlot> {
        let mut map = self
            .branch_queues
            .lock()
            .expect("branch write queue map poisoned");
        if let Some(existing) = map.get(branch) {
            return Arc::clone(existing);
        }
        let fresh = QueueSlot::new(&BRANCH_GATE);
        map.insert(branch.clone(), Arc::clone(&fresh));
        fresh
    }

    /// Acquire the coarse effect gate for one graph branch.
    ///
    /// RFC-022-enrolled callers MUST acquire this before any per-table queue.
    /// It is an in-process contention optimization only; publisher OCC
    /// remains the correctness authority.
    pub(crate) async fn acquire_branch(&self, branch: Option<&str>) -> Result<QueueGuard> {
        refuse_while_preparing("branch")?;
        let key = branch.map(str::to_string);
        Ok(scheduled_lock(self.branch_slot(&key)).await)
    }

    /// Take the shared schema permit, then `branch`'s gate (`None` for
    /// main), in the writer order.
    pub(crate) async fn acquire_branch_write_gates(
        &self,
        branch: Option<&str>,
    ) -> Result<BranchWriteGates> {
        let schema = self.acquire_schema_shared().await?;
        let guard = self.acquire_branch(branch).await?;
        Ok(BranchWriteGates {
            branch_key: branch.map(str::to_string),
            schema,
            branch: guard,
        })
    }

    /// Take the schema gate's shared side: proof that no
    /// contract-lifecycle pass is concurrently swapping the accepted
    /// schema/catalog view. Acquire BEFORE the branch gate and any table
    /// queue; never re-acquire either side while holding a permit (the
    /// gate is non-reentrant — see [`SchemaGateSlot`]).
    pub(crate) async fn acquire_schema_shared(&self) -> Result<SchemaSharedPermit> {
        refuse_while_preparing("schema_shared")?;
        Ok(scheduled_schema_shared(Arc::clone(&self.schema_gate)).await)
    }

    /// Take the schema gate's exclusive side: sole ownership of the
    /// accepted-view transition. Shared holders drain first; new shared
    /// permits queue behind this acquisition in plain mode. Same
    /// non-reentrancy contract as [`Self::acquire_schema_shared`].
    pub(crate) async fn acquire_schema_exclusive(&self) -> Result<SchemaExclusivePermit> {
        refuse_while_preparing("schema_exclusive")?;
        Ok(scheduled_schema_exclusive(Arc::clone(&self.schema_gate)).await)
    }

    /// Reserve the sole immutable export cut without waiting.
    pub(crate) fn try_acquire_export_cut(self: &Arc<Self>) -> Option<ExportCutPermit> {
        let permit = Arc::clone(&self.export_gate).try_write_owned().ok()?;
        observed(&GATE_EXPORT_HOLD);
        Some(ExportCutPermit {
            _manager: Arc::clone(self),
            _permit: permit,
        })
    }

    /// Exclude a live export while destructive control is active.
    ///
    /// Destructive controls take the shared side so unrelated controls keep
    /// their existing concurrency; they only serialize against an export cut.
    pub(crate) fn try_acquire_export_destructive(
        self: &Arc<Self>,
    ) -> Option<ExportDestructivePermit> {
        let permit = Arc::clone(&self.export_gate).try_read_owned().ok()?;
        observed(&GATE_EXPORT_HOLD);
        Some(ExportDestructivePermit {
            _manager: Arc::clone(self),
            _permit: permit,
        })
    }

    /// Acquire several graph-branch control gates in one deterministic order.
    ///
    /// Native branch create-from reads a source ref and mutates a target ref, so
    /// both incarnations must remain stable across its fresh revalidation and
    /// visibility point. Sorting/deduping gives branch control the same
    /// deadlock-free acquisition rule as [`Self::acquire_many`] gives tables.
    pub(crate) async fn acquire_branches(
        &self,
        branches: &[Option<String>],
    ) -> Result<Vec<QueueGuard>> {
        refuse_while_preparing("branch")?;
        if branches.is_empty() {
            return Ok(Vec::new());
        }
        let mut sorted = branches.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut guards = Vec::with_capacity(sorted.len());
        for branch in sorted {
            guards.push(scheduled_lock(self.branch_slot(&branch)).await);
        }
        Ok(guards)
    }

    /// Acquire exclusive access to the queue for one `(table_key, branch)`.
    ///
    /// Blocks until the lock is available. Drop the returned guard to
    /// release; the lock outlives the `WriteQueueManager` borrow.
    pub(crate) async fn acquire(&self, key: &TableQueueKey) -> Result<QueueGuard> {
        refuse_while_preparing("table")?;
        Ok(scheduled_lock(self.slot(key)).await)
    }

    /// Acquire exclusive access to many `(table_key, branch)` keys
    /// atomically, in lex-sorted order. Used by multi-table writers
    /// (mutation finalize, branch_merge, schema apply) so all callers
    /// agree on acquisition order — prevents lock-order inversion.
    ///
    /// Empty input returns an empty Vec without touching the map.
    /// Duplicates in `keys` are deduped before acquisition (the same
    /// key acquired twice would deadlock against itself).
    pub(crate) async fn acquire_many(&self, keys: &[TableQueueKey]) -> Result<Vec<QueueGuard>> {
        refuse_while_preparing("table")?;
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut sorted: Vec<TableQueueKey> = keys.to_vec();
        sorted.sort();
        sorted.dedup();
        let mut guards = Vec::with_capacity(sorted.len());
        for key in &sorted {
            guards.push(self.acquire(key).await?);
        }
        Ok(guards)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::write_queue_root_identity;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use tokio::time::timeout;

    fn key(table: &str, branch: Option<&str>) -> TableQueueKey {
        (table.to_string(), branch.map(str::to_string))
    }

    /// A table gate crosses `acquire`, `hold`, `release` in that order, the
    /// uncontended holder and the contender alike; the contender's `hold`
    /// follows the holder's `release`.
    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn a_table_gate_crosses_acquire_hold_release_in_order() {
        let _scenario = crate::seams::FailScenario::setup();
        let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let note = |name: &'static str| {
            let events = Arc::clone(&events);
            move || events.lock().unwrap().push(name)
        };
        let _acquire = GATE_TABLE_ACQUIRE.observe(note("acquire"));
        let _hold = GATE_TABLE_HOLD.observe(note("hold"));
        let _release = GATE_TABLE_RELEASE.observe(note("release"));
        let crossed = || events.lock().unwrap().clone();

        let qm = Arc::new(WriteQueueManager::new());
        let k = key("t1", None);
        let holder = qm.acquire(&k).await.unwrap();
        assert_eq!(crossed(), ["acquire", "hold"]);

        let qm2 = Arc::clone(&qm);
        let k2 = k.clone();
        let contender = tokio::spawn(async move { qm2.acquire(&k2).await.unwrap() });
        timeout(Duration::from_secs(2), async {
            while crossed().len() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the contender must cross acquire while the holder holds");
        assert_eq!(crossed(), ["acquire", "hold", "acquire"]);

        drop(holder);
        let contended = timeout(Duration::from_secs(2), contender)
            .await
            .expect("the contender must hold once the holder releases")
            .unwrap();
        assert_eq!(crossed(), ["acquire", "hold", "acquire", "release", "hold"]);
        drop(contended);
        assert_eq!(
            crossed(),
            ["acquire", "hold", "acquire", "release", "hold", "release"]
        );
    }

    /// The other gates cross the same way: branch and both schema sides
    /// `acquire, hold, release`; export, which refuses instead of waiting,
    /// `hold, release` for the cut and for a destructive control alike.
    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn branch_schema_and_export_gates_cross_their_seams() {
        let _scenario = crate::seams::FailScenario::setup();
        let events: Arc<Mutex<Vec<&'static str>>> = Arc::new(Mutex::new(Vec::new()));
        let note = |name: &'static str| {
            let events = Arc::clone(&events);
            move || events.lock().unwrap().push(name)
        };
        let _observers = [
            GATE_BRANCH_ACQUIRE.observe(note("branch.acquire")),
            GATE_BRANCH_HOLD.observe(note("branch.hold")),
            GATE_BRANCH_RELEASE.observe(note("branch.release")),
            GATE_SCHEMA_SHARED_ACQUIRE.observe(note("schema_shared.acquire")),
            GATE_SCHEMA_SHARED_HOLD.observe(note("schema_shared.hold")),
            GATE_SCHEMA_SHARED_RELEASE.observe(note("schema_shared.release")),
            GATE_SCHEMA_EXCLUSIVE_ACQUIRE.observe(note("schema_exclusive.acquire")),
            GATE_SCHEMA_EXCLUSIVE_HOLD.observe(note("schema_exclusive.hold")),
            GATE_SCHEMA_EXCLUSIVE_RELEASE.observe(note("schema_exclusive.release")),
            GATE_EXPORT_HOLD.observe(note("export.hold")),
            GATE_EXPORT_RELEASE.observe(note("export.release")),
        ];

        let qm = Arc::new(WriteQueueManager::new());
        drop(qm.acquire_branch(Some("feature")).await.unwrap());
        drop(qm.acquire_schema_shared().await.unwrap());
        drop(qm.acquire_schema_exclusive().await.unwrap());
        drop(qm.try_acquire_export_cut().unwrap());
        drop(qm.try_acquire_export_destructive().unwrap());
        assert_eq!(
            *events.lock().unwrap(),
            [
                "branch.acquire",
                "branch.hold",
                "branch.release",
                "schema_shared.acquire",
                "schema_shared.hold",
                "schema_shared.release",
                "schema_exclusive.acquire",
                "schema_exclusive.hold",
                "schema_exclusive.release",
                "export.hold",
                "export.release",
                "export.hold",
                "export.release",
            ]
        );
    }

    #[tokio::test]
    async fn acquire_many_empty_returns_empty() {
        let qm = WriteQueueManager::new();
        let guards = qm.acquire_many(&[]).await.unwrap();
        assert!(guards.is_empty());
    }

    #[tokio::test]
    async fn acquire_many_dedupes_repeated_keys() {
        // Same key passed twice would deadlock if not deduped.
        let qm = WriteQueueManager::new();
        let k = key("t1", None);
        let guards = timeout(
            Duration::from_secs(2),
            qm.acquire_many(&[k.clone(), k.clone(), k]),
        )
        .await
        .expect("acquire_many with duplicates deadlocked")
        .unwrap();
        assert_eq!(guards.len(), 1);
    }

    #[tokio::test]
    async fn acquire_branches_dedupes_main_and_named_keys() {
        let qm = WriteQueueManager::new();
        let guards = timeout(
            Duration::from_secs(2),
            qm.acquire_branches(&[
                Some("feature".to_string()),
                None,
                Some("feature".to_string()),
                None,
            ]),
        )
        .await
        .expect("duplicate branch keys must not self-deadlock")
        .unwrap();
        assert_eq!(guards.len(), 2);
    }

    #[test]
    fn export_cut_and_destructive_controls_share_one_root_gate() {
        let root = format!("memory://export-gate/{}", ulid::Ulid::new());
        let first = WriteQueueManager::for_root(&root);
        let second = WriteQueueManager::for_root(&root);

        let destructive_a = first
            .try_acquire_export_destructive()
            .expect("first destructive control must acquire");
        let destructive_b = second
            .try_acquire_export_destructive()
            .expect("destructive controls must remain mutually concurrent");
        assert!(second.try_acquire_export_cut().is_none());
        drop(destructive_a);
        drop(destructive_b);

        let cut = first
            .try_acquire_export_cut()
            .expect("export must acquire after destructive controls release");
        assert!(second.try_acquire_export_cut().is_none());
        assert!(second.try_acquire_export_destructive().is_none());
        drop(cut);

        assert!(second.try_acquire_export_cut().is_some());
    }

    /// Shared permits are concurrent; each side excludes the other. The
    /// contending acquisitions run on their own tasks, as engine callers do.
    #[tokio::test]
    async fn schema_exclusive_excludes_shared_both_directions() {
        let qm = Arc::new(WriteQueueManager::new());

        let first = qm.acquire_schema_shared().await.unwrap();
        let qm2 = Arc::clone(&qm);
        let second = timeout(
            Duration::from_secs(2),
            tokio::spawn(async move { qm2.acquire_schema_shared().await.unwrap() }),
        )
        .await
        .expect("a second shared permit must not wait behind the first")
        .unwrap();
        drop(first);
        drop(second);

        // Held exclusive blocks a shared acquire.
        let exclusive = qm.acquire_schema_exclusive().await.unwrap();
        let qm2 = Arc::clone(&qm);
        let blocked = timeout(
            Duration::from_millis(200),
            tokio::spawn(async move { qm2.acquire_schema_shared().await.unwrap() }),
        )
        .await;
        assert!(
            blocked.is_err(),
            "a shared permit must wait behind a held exclusive permit"
        );
        drop(exclusive);
        let qm2 = Arc::clone(&qm);
        let shared = timeout(
            Duration::from_secs(2),
            tokio::spawn(async move { qm2.acquire_schema_shared().await.unwrap() }),
        )
        .await
        .expect("shared must acquire once the exclusive permit releases")
        .unwrap();

        // Held shared blocks an exclusive acquire.
        let qm2 = Arc::clone(&qm);
        let blocked = timeout(
            Duration::from_millis(200),
            tokio::spawn(async move { qm2.acquire_schema_exclusive().await.unwrap() }),
        )
        .await;
        assert!(
            blocked.is_err(),
            "an exclusive permit must wait behind a held shared permit"
        );
        drop(shared);
        let qm2 = Arc::clone(&qm);
        let _exclusive = timeout(
            Duration::from_secs(2),
            tokio::spawn(async move { qm2.acquire_schema_exclusive().await.unwrap() }),
        )
        .await
        .expect("exclusive must acquire once the shared permit releases")
        .unwrap();
    }

    /// The plain-mode no-starvation pin: a shared acquisition after a queued
    /// exclusive parks behind it (tokio's `RwLock` is write-preferring). A
    /// tokio change of policy reds this and the RFC's fairness claim.
    #[tokio::test]
    async fn queued_schema_exclusive_blocks_later_shared() {
        let qm = Arc::new(WriteQueueManager::new());
        let held_shared = qm.acquire_schema_shared().await.unwrap();

        let qm_writer = Arc::clone(&qm);
        let writer =
            tokio::spawn(async move { qm_writer.acquire_schema_exclusive().await.unwrap() });
        let qm_probe = Arc::clone(&qm);
        timeout(Duration::from_secs(2), async move {
            while let Ok(permit) = Arc::clone(&qm_probe.schema_gate.lock).try_read_owned() {
                drop(permit);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the exclusive acquisition must enter the waiter queue");

        let qm2 = Arc::clone(&qm);
        let overtaking = timeout(
            Duration::from_millis(200),
            tokio::spawn(async move { qm2.acquire_schema_shared().await.unwrap() }),
        )
        .await;
        assert!(
            overtaking.is_err(),
            "a shared permit requested after a queued exclusive must park behind it"
        );

        drop(held_shared);
        let exclusive = timeout(Duration::from_secs(2), writer)
            .await
            .expect("queued exclusive must acquire once the shared permit releases")
            .expect("writer task must not panic");
        drop(exclusive);
        let qm2 = Arc::clone(&qm);
        let _shared = timeout(
            Duration::from_secs(2),
            tokio::spawn(async move { qm2.acquire_schema_shared().await.unwrap() }),
        )
        .await
        .expect("the parked shared permit must acquire after the exclusive releases")
        .unwrap();
    }

    #[tokio::test]
    async fn schema_gate_shared_across_for_root_handles() {
        let root = format!("memory://schema-gate/{}", ulid::Ulid::new());
        let first = WriteQueueManager::for_root(&root);
        let second = WriteQueueManager::for_root(&root);

        let exclusive = first.acquire_schema_exclusive().await.unwrap();
        let second2 = Arc::clone(&second);
        let blocked = timeout(
            Duration::from_millis(200),
            tokio::spawn(async move { second2.acquire_schema_shared().await.unwrap() }),
        )
        .await;
        assert!(
            blocked.is_err(),
            "handles for one root must exclude on one schema gate"
        );
        drop(exclusive);
        let _shared = timeout(
            Duration::from_secs(2),
            tokio::spawn(async move { second.acquire_schema_shared().await.unwrap() }),
        )
        .await
        .expect("the second handle's shared permit must acquire after release")
        .unwrap();
    }

    #[tokio::test]
    async fn acquire_many_sorts_keys_deterministically() {
        // Two callers passing keys in different orders must acquire in
        // the same internal order. We test this indirectly: caller A
        // passes [a, c] and caller B passes [c, a]; if they both
        // acquire in sorted order the second caller blocks on `a` first,
        // not `c` — same as A — so no deadlock under any interleaving.
        // Direct sort observation: call acquire_many with a reversed
        // input and verify it doesn't deadlock against a held guard on
        // the sorted-first key.
        let qm = Arc::new(WriteQueueManager::new());
        let a = key("a", None);
        let z = key("z", None);

        // Hold `a` exclusively.
        let _held = qm.acquire(&a).await.unwrap();

        // acquire_many([z, a]) — must sort to [a, z] internally and
        // block on `a`. With a 200ms timeout we should NOT see it
        // complete (it's blocked on `a`).
        let qm2 = Arc::clone(&qm);
        let z_clone = z.clone();
        let a_clone = a.clone();
        let result = timeout(Duration::from_millis(200), async move {
            qm2.acquire_many(&[z_clone, a_clone]).await.unwrap()
        })
        .await;
        assert!(
            result.is_err(),
            "acquire_many should block on `a`, the lex-first key"
        );
    }

    #[tokio::test]
    async fn same_key_acquire_serializes() {
        let qm = Arc::new(WriteQueueManager::new());
        let k = key("t1", None);

        let first = qm.acquire(&k).await.unwrap();

        // Second acquire on same key should NOT complete within 200ms.
        let qm2 = Arc::clone(&qm);
        let k2 = k.clone();
        let blocked = timeout(Duration::from_millis(200), async move {
            qm2.acquire(&k2).await.unwrap()
        })
        .await;
        assert!(blocked.is_err(), "second acquire on same key must block");

        // Drop the first guard, then second acquire should succeed.
        drop(first);
        let _second = timeout(Duration::from_secs(2), qm.acquire(&k))
            .await
            .expect("second acquire after release should not block");
    }

    #[tokio::test]
    async fn disjoint_keys_acquire_concurrently() {
        let qm = Arc::new(WriteQueueManager::new());
        let a = key("a", None);
        let b = key("b", None);

        // Hold `a` indefinitely.
        let _held_a = qm.acquire(&a).await.unwrap();

        // Acquire `b` on a different task. Should complete promptly
        // because `b` is disjoint from `a`.
        let qm2 = Arc::clone(&qm);
        let start = Instant::now();
        let _held_b = timeout(Duration::from_secs(2), qm2.acquire(&b))
            .await
            .expect("disjoint key acquire must not block on unrelated held key");
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "disjoint acquire took {:?}, should be near-instant",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn disjoint_branches_on_same_table_do_not_serialize() {
        // (table, main) and (table, feature) are different keys.
        let qm = Arc::new(WriteQueueManager::new());
        let main_k = key("t1", None);
        let feature_k = key("t1", Some("feature"));

        let _held_main = qm.acquire(&main_k).await.unwrap();
        let _held_feature = timeout(Duration::from_secs(2), qm.acquire(&feature_k))
            .await
            .expect("same-table-different-branch should not serialize");
    }

    #[test]
    fn opaque_root_registry_shares_manager_across_handles() {
        let root = format!("memory://write-queue-registry/{}", ulid::Ulid::new());
        let first = WriteQueueManager::for_root(&root);
        let second = WriteQueueManager::for_root(&root);
        assert!(Arc::ptr_eq(&first, &second));

        let other = WriteQueueManager::for_root(&format!("{root}/other"));
        assert!(!Arc::ptr_eq(&first, &other));
    }

    #[test]
    fn relative_and_absolute_local_roots_share_manager() {
        let relative = PathBuf::from("target")
            .join("write-queue-identities")
            .join(ulid::Ulid::new().to_string())
            .join("graph.omni");
        let absolute = std::env::current_dir().unwrap().join(&relative);
        let relative_identity = write_queue_root_identity(relative.to_str().unwrap()).unwrap();
        let absolute_identity = write_queue_root_identity(absolute.to_str().unwrap()).unwrap();

        assert_eq!(relative_identity, absolute_identity);
        let first = WriteQueueManager::for_root(&relative_identity);
        let second = WriteQueueManager::for_root(&absolute_identity);
        assert!(Arc::ptr_eq(&first, &second));
    }

    /// Issue #1369: a write sent back prepares again holding its branch's
    /// writer gates, which are not reentrant. Inside a preparation every
    /// writer-gate acquisition answers with an internal error instead of
    /// waiting on the attempt's own gate; from the attempt's commit point on,
    /// and outside any preparation, the gates are taken as before.
    #[tokio::test]
    async fn a_writer_gate_taken_while_preparing_is_refused_issue_1369() {
        macro_rules! refused {
            ($acquisition:expr) => {{
                let error = timeout(Duration::from_secs(2), $acquisition)
                    .await
                    .expect("a writer gate taken while preparing waited on its own gate")
                    .err()
                    .expect("a writer gate taken while preparing is refused");
                assert!(
                    error.to_string().contains("while preparing an attempt"),
                    "{error}"
                );
            }};
        }

        let qm = WriteQueueManager::new();
        let table = key("node:Person", None);
        // The gates a gated attempt holds from its preparation through its
        // publication (`PreEffectReprepares::prepare_again`).
        let held = qm.acquire_branch_write_gates(None).await.unwrap();
        preparing(async {
            assert!(is_preparing());
            refused!(qm.acquire_branch_write_gates(None));
            refused!(qm.acquire_schema_shared());
            refused!(qm.acquire_schema_exclusive());
            refused!(qm.acquire_branch(None));
            refused!(qm.acquire_branches(&[None]));
            refused!(qm.acquire(&table));
            refused!(qm.acquire_many(std::slice::from_ref(&table)));
            refused!(qm.acquire_many(&[]));
            // The commit point ends the preparation, and the attempt
            // completes its envelope with the table gates.
            assert!(end_preparation());
            assert!(!is_preparing());
            drop(qm.acquire_many(std::slice::from_ref(&table)).await.unwrap());
        })
        .await;
        drop(held);
        assert!(!is_preparing());
        assert!(!end_preparation(), "no preparation outside the scope");
        drop(qm.acquire_branch_write_gates(None).await.unwrap());
    }

    #[cfg(unix)]
    #[test]
    fn real_and_symlinked_local_roots_share_manager_before_init() {
        use std::os::unix::fs::symlink;

        let parent = tempfile::tempdir().unwrap();
        let real_parent = parent.path().join("real");
        let alias_parent = parent.path().join("alias");
        std::fs::create_dir(&real_parent).unwrap();
        symlink(&real_parent, &alias_parent).unwrap();

        // The graph suffix deliberately does not exist: init computes its
        // queue identity before creating the graph directory.
        let real_root = real_parent.join("future").join("graph.omni");
        let alias_root = alias_parent.join("future").join("graph.omni");
        let real_identity = write_queue_root_identity(real_root.to_str().unwrap()).unwrap();
        let alias_identity = write_queue_root_identity(alias_root.to_str().unwrap()).unwrap();

        assert_eq!(real_identity, alias_identity);
        let first = WriteQueueManager::for_root(&real_identity);
        let second = WriteQueueManager::for_root(&alias_identity);
        assert!(Arc::ptr_eq(&first, &second));
    }
}
