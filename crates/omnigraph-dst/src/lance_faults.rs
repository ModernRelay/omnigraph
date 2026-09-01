//! Lance-realm fault injection.
//!
//! `FaultPlan` weather reaches two realms: omnigraph's `StorageAdapter`
//! (manifest / write queue / sidecars) via `FailingStorage`, and — this
//! module — Lance's own table IO (data files, txn files, its commit
//! protocol), which resolves through the engine's process-wide Lance
//! `ObjectStoreRegistry`. It interposes a wrapping provider over the
//! registry's `shared-memory` scheme (the registry IS the seam — zero
//! Lance changes):
//! the provider delegates store construction to the original provider, then
//! swaps the store's `inner` for a decorator that consults the active
//! universe's [`LanceFaultState`] on every call.
//!
//! Discipline mirrors the adapter-realm injector (`FailingStorage`):
//! - the state's OWN SplitMix64 stream (derived from `FaultPlan.seed` with a
//!   realm salt, so both realms ride one seed tree without sharing draws);
//! - marked errors (`FAULT_MARKER`) so the workload classifies injected
//!   failures as legal rejections;
//! - enable gate (init + fixture load run clean) and oracle suspension,
//!   both forwarded from `FailingStorage` so every existing call site
//!   toggles both realms at once;
//! - latency charged in VIRTUAL time (`tokio::time::sleep`) when the
//!   runtime's clock is paused — the in-suite universes; the lane B
//!   child runs an unpaused runtime, so its weather latency sleeps real
//!   (bounded) milliseconds.
//!
//! The provider is installed once per process ([`install`]); with no active
//! state (or a disabled one) it is a pure passthrough, so clean universes
//! and non-fault tests are unaffected.

use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance_io::object_store::{
    ObjectStore as LanceObjectStore, ObjectStoreParams, ObjectStoreProvider,
};
use object_store::ObjectStoreExt as _;
use object_store::path::Path as OsPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use url::Url;

use crate::harness::{FAULT_MARKER, FaultPlan, KillState};
use crate::rand::SplitMix64;

/// Realm salt: the Lance-realm stream derives from the same plan seed as the
/// adapter realm but never shares draws with it (call orders differ between
/// realms; separate streams keep each realm's sequence self-consistent).
const LANCE_REALM_SALT: u64 = 0x4C41_4E43_455F_4453;

/// Per-universe fault state for the Lance realm. Same knobs and gates as the
/// adapter-realm `FailingStorage`, consulted globally by the installed
/// provider's stores (the registry outlives universes; the state does not).
///
/// SELF-SYNCHRONIZING randomness (first-run lesson): a single sequential
/// stream made the fault decisions a function of GLOBAL call order, and the
/// known ~2% residual scheduling flip reorders Lance-internal calls — one
/// swapped call cascaded into a different injection set and broke replay
/// identity. Each decision now derives from
/// `(seed, op, location, nth-call-to-that-(op,location))`, so a benign
/// reorder of DIFFERENT calls changes nothing and replay is robust to the
/// residual (same principle as the history oracle's self-synchronizing
/// recording).
#[derive(Debug)]
pub struct LanceFaultState {
    seed: u64,
    /// Per-(op, location) call counters — the self-synchronizing index.
    counters: Mutex<std::collections::HashMap<(String, String), u64>>,
    error_pct: u64,
    read_error_pct: u64,
    latency_pct: u64,
    max_latency_ms: u64,
    enabled: AtomicBool,
    suspended: AtomicBool,
    injected: AtomicUsize,
}

/// FNV-1a — tiny, dependency-free, deterministic across processes (never
/// `DefaultHasher`, whose keys are per-process random).
fn fnv1a(s: &str) -> u64 {
    let mut h: u64 = 0xCBF2_9CE4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01B3);
    }
    h
}

impl LanceFaultState {
    pub fn from_plan(plan: &FaultPlan) -> Arc<Self> {
        Arc::new(Self {
            seed: plan.seed ^ LANCE_REALM_SALT,
            counters: Mutex::new(std::collections::HashMap::new()),
            error_pct: plan.error_pct,
            read_error_pct: plan.read_error_pct,
            latency_pct: plan.latency_pct,
            max_latency_ms: plan.max_latency_ms,
            enabled: AtomicBool::new(false),
            suspended: AtomicBool::new(false),
            injected: AtomicUsize::new(0),
        })
    }

    pub fn enable(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub fn suspend(&self) {
        self.suspended.store(true, Ordering::SeqCst);
    }

    pub fn resume(&self) {
        self.suspended.store(false, Ordering::SeqCst);
    }

    /// Injected-error count for the whole universe (report evidence that the
    /// realm actually saw weather).
    pub fn injected(&self) -> usize {
        self.injected.load(Ordering::SeqCst)
    }

    fn active(&self) -> bool {
        self.enabled.load(Ordering::SeqCst) && !self.suspended.load(Ordering::SeqCst)
    }

    async fn fault(&self, read: bool, op: &str, location: &str) -> object_store::Result<()> {
        if !self.active() {
            return Ok(());
        }
        let (err_roll, lat_roll, lat_ms) = {
            let n = {
                let mut counters = self.counters.lock().unwrap();
                let slot = counters
                    .entry((op.to_string(), location.to_string()))
                    .or_insert(0);
                let v = *slot;
                *slot += 1;
                v
            };
            let mut rng = SplitMix64(
                self.seed
                    ^ fnv1a(op)
                    ^ fnv1a(location).rotate_left(31)
                    ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15),
            );
            (
                rng.below(100),
                rng.below(100),
                1 + rng.below(self.max_latency_ms.max(1)),
            )
        };
        if lat_roll < self.latency_pct {
            tokio::time::sleep(Duration::from_millis(lat_ms)).await;
        }
        let pct = if read {
            self.read_error_pct
        } else {
            self.error_pct
        };
        if err_roll < pct {
            self.injected.fetch_add(1, Ordering::SeqCst);
            return Err(object_store::Error::Generic {
                store: "dst-lance-realm",
                source: format!("{FAULT_MARKER}: lance {op} {location}").into(),
            });
        }
        Ok(())
    }
}

/// The active universe's Lance-realm state. Written by `run_universe`
/// (unconditionally at universe start, cleared at end); read by every store
/// call. Universes are sequential per process, so one slot suffices.
static ACTIVE: RwLock<Option<Arc<LanceFaultState>>> = RwLock::new(None);

pub fn set_active(state: Option<Arc<LanceFaultState>>) {
    *ACTIVE.write().unwrap() = state;
}

fn active_state() -> Option<Arc<LanceFaultState>> {
    ACTIVE.read().unwrap().clone()
}

/// The concurrent universe's arbiter,
/// reachable from this realm's decorator: (scheduler, writers count). Set
/// by `run_concurrent_universe` when `seam_schedule` is on, cleared before
/// the final audit. With the slot empty every call passes ungated (zero
/// change for sequential universes and clean tests).
static SEAM_SCHED: RwLock<Option<(Arc<crate::concurrent::SeamScheduler>, usize)>> =
    RwLock::new(None);

pub fn set_seam_scheduler(s: Option<(Arc<crate::concurrent::SeamScheduler>, usize)>) {
    *SEAM_SCHED.write().unwrap() = s;
}

fn seam_scheduler() -> Option<(Arc<crate::concurrent::SeamScheduler>, usize)> {
    SEAM_SCHED.read().unwrap().clone()
}

/// Thread-name attribution: the actors' OS threads carry their
/// identities — `dst-writer-N` (scheduler id N), `dst-branch-actor` (id =
/// writers), `dst-maintenance` (id = writers+1). Lance-realm calls executed
/// INLINE on an actor's thread inherit its name and take turns; calls from
/// Lance's own pool threads (lance-cpu, lance-io) carry other names and run
/// UNGATED — the measured coverage gap (`note_unattributed`), never a
/// silent one.
fn actor_from_thread(writers: usize) -> Option<usize> {
    let t = std::thread::current();
    let name = t.name()?;
    if let Some(n) = name.strip_prefix("dst-writer-") {
        return n.parse::<usize>().ok();
    }
    match name {
        "dst-branch-actor" => Some(writers),
        "dst-maintenance" => Some(writers + 1),
        _ => None,
    }
}

/// The VIOLATION-TIER CANARY (Lance-realm bytes verb):
/// targeted one-shot corruption of a `get` RESPONSE — the cell where
/// silent acceptance is structurally possible, because the persisted-tier
/// census measured Lance has NO checksums on data pages or manifests.
/// The canary flips one seeded byte in the nth response whose path
/// contains `substring`; the instrument then proves the flipped byte
/// either reds a channel (silent lie CAUGHT — the canary's purpose) or
/// hits structural validation (the detection map fills). Slot-based and
/// default-None: zero cost and zero draw impact for every other test.
pub struct BytesCanary {
    pub substring: String,
    /// Corrupt the nth matching read (0-based).
    pub nth: usize,
    /// Seeded byte position (reduced modulo the payload length).
    pub offset_seed: u64,
    hits: AtomicUsize,
    fired_at: Mutex<Option<(String, usize)>>,
}

impl BytesCanary {
    pub fn new(substring: &str, nth: usize, offset_seed: u64) -> Arc<Self> {
        Arc::new(Self {
            substring: substring.to_string(),
            nth,
            offset_seed,
            hits: AtomicUsize::new(0),
            fired_at: Mutex::new(None),
        })
    }
    /// (path, byte offset) of the delivered lie, if it fired.
    pub fn fired_at(&self) -> Option<(String, usize)> {
        self.fired_at.lock().unwrap().clone()
    }
}

static BYTES_CANARY: RwLock<Option<Arc<BytesCanary>>> = RwLock::new(None);

pub fn set_bytes_canary(c: Option<Arc<BytesCanary>>) {
    *BYTES_CANARY.write().unwrap() = c;
}

fn bytes_canary() -> Option<Arc<BytesCanary>> {
    BYTES_CANARY.read().unwrap().clone()
}

/// the crash-state enumeration's Lance-realm hook — the same
/// `KillState` the adapter realm consults, so ONE counter sees every
/// durable write. Set/cleared by `run_universe` alongside `ACTIVE`.
static KILL: RwLock<Option<Arc<KillState>>> = RwLock::new(None);

pub fn set_kill(state: Option<Arc<KillState>>) {
    *KILL.write().unwrap() = state;
}

fn kill_state() -> Option<Arc<KillState>> {
    KILL.read().unwrap().clone()
}

/// Completion-cut hook, Lance realm — the mirror of
/// `FailingStorage::count_completion`: call AFTER the inner store
/// confirmed the write.
fn count_lance_completion(op: &str, location: &str) {
    if let Some(k) = kill_state() {
        k.on_completion(op, location);
    }
}

async fn fault(
    read: bool,
    op: &str,
    location: &str,
) -> object_store::Result<Option<crate::concurrent::SeamGuard>> {
    // The arbiter's turn comes FIRST and the returned guard is held by the
    // CALLER across delegation, so the granted turn spans the fault decision
    // AND the store effect. Unattributed threads take no turn and consume no
    // draws (see `SeamScheduler::lance_turns`).
    let turn = match seam_scheduler() {
        Some((sched, writers)) => match actor_from_thread(writers) {
            Some(actor) => {
                // Metadata rides in so the directed hold (the park-the-deleter recipe)
                // can pattern-match branch-refs traffic.
                let g = sched.enter_call(actor, op, location);
                if g.is_some() {
                    sched.note_lance_turn();
                    // Trace probe: printed AFTER the grant, so
                    // lines appear in true serialization order.
                    if location.contains("_refs") && std::env::var("DST_HOLD_TRACE").is_ok() {
                        println!("dst hold refs [actor={actor} op={op} loc={location}]");
                    }
                }
                g
            }
            None => {
                sched.note_unattributed();
                None
            }
        },
        None => None,
    };
    // Death first (a dead process performs nothing), fault rolls second,
    // the kill COUNT last — a fault-rejected call never reaches the store,
    // so it is not a crash-distinguishable durable write (mirrors the
    // adapter realm's ordering; audit improvement 2026-08-12).
    if let Some(k) = kill_state()
        && let Err(msg) = k.refuse_if_dead(op, location)
    {
        return Err(object_store::Error::Generic {
            store: "dst-kill",
            source: msg.into(),
        });
    }
    if !read {
        let counting = kill_state().map(|k| k.counting()).unwrap_or(false);
        crate::write_census::record("lance", op, location, counting);
    }
    if let Some(state) = active_state() {
        state.fault(read, op, location).await?;
    }
    if !read
        && let Some(k) = kill_state()
        && let Err(msg) = k.on_write(op, location)
    {
        return Err(object_store::Error::Generic {
            store: "dst-kill",
            source: msg.into(),
        });
    }
    Ok(turn)
}

static INSTALLED: OnceLock<()> = OnceLock::new();

/// WRITE CENSUS bottom-count: every key the Lance realm's store currently
/// holds for `root`, flat. Constructed through the registry provider (the
/// same route the engine takes), listed with no prefix filter — the store
/// contents are ground truth regardless of the read path, so this is the
/// from-below half of the census reconciliation for the table realm.
/// SHARED-MEMORY SCHEME ONLY (its one caller is the in-suite census);
/// pointing the census at a lane B file root needs a scheme parameter.
/// EVERY failure arm PANICS — construction errors AND per-item listing
/// errors: a partial or empty listing would make the census's
/// store-keys-subset-of-recorded check vacuously green, which is the
/// silent-success failure mode of an honesty instrument.
///
/// # Panics
/// On an unparseable root, failed store construction, or any errored
/// listing item.
pub(crate) async fn list_realm_keys(root: &str) -> Vec<String> {
    let registry = omnigraph::dst_lance_store_registry();
    let provider = registry
        .get_provider("shared-memory")
        .expect("census bottom listing: shared-memory provider missing from the registry");
    let url = Url::parse(root)
        .unwrap_or_else(|e| panic!("census bottom listing: unparseable root {root}: {e}"));
    let store = provider
        .new_store(url, &ObjectStoreParams::default())
        .await
        .unwrap_or_else(|e| panic!("census bottom listing: store construction failed: {e}"));
    let mut out = Vec::new();
    let mut stream = store.inner.list(None);
    while let Some(item) = stream.next().await {
        let meta =
            item.unwrap_or_else(|e| panic!("census bottom listing: errored listing item: {e}"));
        out.push(meta.location.to_string());
    }
    out
}

/// Interpose the fault-injecting provider over the engine registry's
/// `shared-memory` provider. Idempotent; process-permanent.
pub fn install() {
    INSTALLED.get_or_init(|| {
        let registry = omnigraph::dst_lance_store_registry();
        let original = registry
            .get_provider("shared-memory")
            .expect("lance registry always has a shared-memory provider");
        registry.insert(
            "shared-memory",
            Arc::new(FaultInjectingProvider { inner: original }),
        );
    });
}

static INSTALLED_FILE: OnceLock<()> = OnceLock::new();

/// LANE B whitebox: interpose the same decorator over the `file` scheme
/// provider — `install()` covers only `shared-memory`, so a local-FS
/// child's Lance-realm writes would otherwise bypass the kill counter.
/// Idempotent; process-permanent; only the dst_child binary calls it.
///
/// # Panics
/// When the registry has no `file` provider (it always ships one).
pub fn install_file() {
    INSTALLED_FILE.get_or_init(|| {
        let registry = omnigraph::dst_lance_store_registry();
        let original = registry
            .get_provider("file")
            .expect("lance registry always has a file provider");
        registry.insert("file", Arc::new(FaultInjectingProvider { inner: original }));
    });
}

/// Wraps the original `shared-memory` provider: same store, same path and
/// prefix semantics, but the constructed store's `inner` is decorated.
#[derive(Debug)]
struct FaultInjectingProvider {
    inner: Arc<dyn ObjectStoreProvider>,
}

#[async_trait]
impl ObjectStoreProvider for FaultInjectingProvider {
    async fn new_store(
        &self,
        base_path: Url,
        params: &ObjectStoreParams,
    ) -> lance_core::Result<LanceObjectStore> {
        let mut store = self.inner.new_store(base_path, params).await?;
        store.inner = Arc::new(FaultInjectingOsStore {
            inner: Arc::clone(&store.inner),
        });
        Ok(store)
    }

    fn extract_path(&self, url: &Url) -> lance_core::Result<OsPath> {
        self.inner.extract_path(url)
    }

    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&std::collections::HashMap<String, String>>,
    ) -> lance_core::Result<String> {
        self.inner
            .calculate_object_store_prefix(url, storage_options)
    }
}

/// The decorator over the `object_store::ObjectStore` trait: fault roll
/// first, then delegate. Defaulted trait methods (`get_ranges`,
/// `list_with_offset`, `rename_opts`, plus the `ObjectStoreExt` surface)
/// route through these required methods, so every Lance-realm call rolls.
/// Multipart uploads roll at initiation only (per-part faults are a future
/// refinement).
#[derive(Debug)]
struct FaultInjectingOsStore {
    inner: Arc<dyn object_store::ObjectStore>,
}

impl fmt::Display for FaultInjectingOsStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DstFaultInjecting({})", self.inner)
    }
}

#[async_trait]
impl object_store::ObjectStore for FaultInjectingOsStore {
    async fn put_opts(
        &self,
        location: &OsPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        crate::cost::tally("l.put", payload.content_length() as u64);
        let _in_flight = kill_state().map(|k| k.enter_write());
        let _turn = fault(false, "put", location.as_ref()).await?;
        let out = self.inner.put_opts(location, payload, opts).await;
        if out.is_ok() {
            count_lance_completion("put", location.as_ref());
        }
        out
    }

    async fn put_multipart_opts(
        &self,
        location: &OsPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        crate::cost::tally("l.put_multipart", 0);
        let _turn = fault(false, "put_multipart", location.as_ref()).await?;
        assert!(
            kill_state().is_none(),
            "multipart upload under completion-cut counting: per-part writes are \
             NOT counted or faulted; build the per-part MultipartUpload hook \
             before any lane B workload crosses the multipart threshold"
        );
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &OsPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let _turn = fault(true, "get", location.as_ref()).await?;
        let result = self.inner.get_opts(location, options).await?;
        crate::cost::tally("l.get", result.meta.size);
        // The violation-tier canary: flip one seeded byte in
        // the targeted response. Rebuild the GetResult around the mutated
        // payload (same meta/range/attributes — a LIE, not a truncation).
        if let Some(canary) = bytes_canary()
            && location.as_ref().contains(&canary.substring)
        {
            let hit = canary.hits.fetch_add(1, Ordering::SeqCst);
            if hit == canary.nth {
                let meta = result.meta.clone();
                let range = result.range.clone();
                let attributes = result.attributes.clone();
                let mut data = result.bytes().await?.to_vec();
                if !data.is_empty() {
                    let off = (canary.offset_seed as usize) % data.len();
                    data[off] ^= 0xFF;
                    *canary.fired_at.lock().unwrap() = Some((location.to_string(), off));
                }
                let corrupted = bytes::Bytes::from(data);
                return Ok(GetResult {
                    payload: object_store::GetResultPayload::Stream(
                        futures::stream::once(async move { Ok(corrupted) }).boxed(),
                    ),
                    meta,
                    range,
                    attributes,
                });
            }
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<OsPath>>,
    ) -> BoxStream<'static, object_store::Result<OsPath>> {
        // PER-ITEM rolls (audit improvement 2026-08-12): a stream delete
        // performs one durable effect PER OBJECT. Initiation-only injection
        // collapsed a whole cleanup into a single all-or-nothing state —
        // mid-stream crash states (PARTIAL cleanup) were unmanufacturable
        // and every k landing in a bulk delete judged the same world.
        // Item-by-item delegation preserves the memory store's semantics
        // (its bulk path is sequential individual deletions) while giving
        // faults and the kill counter one decision per object.
        let inner = Arc::clone(&self.inner);
        locations
            .then(move |path_res| {
                let inner = Arc::clone(&inner);
                async move {
                    let path = path_res?;
                    crate::cost::tally("l.delete", 0);
                    let _in_flight = kill_state().map(|k| k.enter_write());
                    let _turn = fault(false, "delete", path.as_ref()).await?;
                    inner.delete(&path).await?;
                    count_lance_completion("delete", path.as_ref());
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&OsPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.cloned();
        futures::stream::once(async move {
            let label = prefix.as_ref().map(|p| p.to_string()).unwrap_or_default();
            match fault(true, "list", &label).await {
                Ok(_turn) => {
                    crate::cost::tally("l.list", 0);
                    inner.list(prefix.as_ref())
                }
                Err(e) => futures::stream::once(futures::future::ready(Err(e))).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&OsPath>,
    ) -> object_store::Result<ListResult> {
        let label = prefix.map(|p| p.to_string()).unwrap_or_default();
        crate::cost::tally("l.list", 0);
        let _turn = fault(true, "list_with_delimiter", &label).await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &OsPath,
        to: &OsPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        crate::cost::tally("l.copy", 0);
        let _in_flight = kill_state().map(|k| k.enter_write());
        let _turn = fault(false, "copy", from.as_ref()).await?;
        let out = self.inner.copy_opts(from, to, options).await;
        if out.is_ok() {
            count_lance_completion("copy", from.as_ref());
        }
        out
    }
}
