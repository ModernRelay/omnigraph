//! WAL research prototype (not a production path): a record log in front of
//! `__manifest` publication.
//!
//! With `OMNIGRAPH_WAL=1`, a head-advancing publication on a branch whose
//! writer epoch this process holds writes one self-contained object, the
//! ***record*** of `__manifest` version `V`, with a conditional create:
//!
//! ```text
//! {graph root}/__wal/{native ref or "main"}/{V:020}.rec
//! ```
//!
//! The record holds the complete row set version `V` will hold (Arrow IPC of
//! the hot rows, and of the cold registry rows the publication pushes, when
//! it pushes any). Its conditional create is the commit point: one request,
//! where the Lance commit of `__manifest` is three dependent ones (hot-row
//! data file, transaction file, manifest). Version numbers are unchanged:
//! record `V` is exactly `__manifest` version `V` before it is folded, so two
//! writers that captured the same head contend on the same object name, as
//! they contend on `_versions/V.manifest` today.
//!
//! The process keeps its branch's ***tail*** in memory: the latest version,
//! its rows, the latest folded `__manifest` dataset and the records not yet
//! folded. The publisher builds the next version from the tail, so a write
//! costs no `__manifest` read, and the coordinator installs `V` at once, so
//! every read in this process sees an acknowledged write when its record is
//! durable. A background ***fold*** writes the `__manifest` Lance versions in
//! order from the records (each record's rows exactly, through the ordinary
//! `commit::overwrite`), so every surface that reads `__manifest` history
//! sees the same versions it would have seen, later.
//!
//! What the prototype does not do (see the research report): a reader in
//! another process reads the tail from storage only through
//! [`read_storage_tail`] (wired into opens, not into every probe); a claim
//! does not write a fencing record; records are kept after their fold;
//! cleanup ignores them.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

use arrow_array::RecordBatch;
use bytes::Bytes;
use lance::Dataset;
use lance_io::object_store::ObjectStore;
use object_store::ObjectStoreExt as _;
use object_store::path::Path;

use crate::error::{OmniError, Result};
use crate::state::{LineageRegistry, ManifestRows};

/// Whether publications go through the record log (`OMNIGRAPH_WAL=1`).
pub fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("OMNIGRAPH_WAL").is_ok_and(|value| value == "1"))
}

/// How many records may wait for their fold before an append waits for the
/// fold to catch up: the bound on the tail's length and on the memory the
/// unfolded rows hold.
fn max_unfolded() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("OMNIGRAPH_WAL_MAX_UNFOLDED")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(64)
    })
}

fn wal_uri(root_uri: &str, native: Option<&str>) -> String {
    format!(
        "{}/__wal/{}",
        root_uri.trim_end_matches('/'),
        native.unwrap_or("main")
    )
}

fn record_name(version: u64) -> String {
    format!("{version:020}.rec")
}

#[derive(Clone)]
struct Pending {
    version: u64,
    rows: Arc<ManifestRows>,
    cold: Option<Arc<LineageRegistry>>,
}

struct TailState {
    tip_version: u64,
    tip_rows: Arc<ManifestRows>,
    folded: Dataset,
    pending: VecDeque<Pending>,
    failed: Option<String>,
}

/// One branch's record log in this process.
pub struct Tail {
    native: Option<String>,
    dead: std::sync::atomic::AtomicBool,
    store: Arc<ObjectStore>,
    base: Path,
    state: Mutex<TailState>,
    wake: tokio::sync::Notify,
    progress: tokio::sync::Notify,
}

type TailKey = (String, Option<String>);

static TAILS: LazyLock<Mutex<HashMap<TailKey, Arc<Tail>>>> = LazyLock::new(Mutex::default);

fn key(root_uri: &str, branch: Option<&str>) -> TailKey {
    (
        root_uri.trim_end_matches('/').to_string(),
        branch.filter(|name| *name != "main").map(str::to_string),
    )
}

/// Research instrument: unfolded records seen at each append (sum, count,
/// max), the fold lag under load.
pub mod lag {
    use std::sync::atomic::{AtomicU64, Ordering};
    pub static SUM: AtomicU64 = AtomicU64::new(0);
    pub static COUNT: AtomicU64 = AtomicU64::new(0);
    pub static MAX: AtomicU64 = AtomicU64::new(0);
    pub fn record(unfolded: u64) {
        SUM.fetch_add(unfolded, Ordering::Relaxed);
        COUNT.fetch_add(1, Ordering::Relaxed);
        MAX.fetch_max(unfolded, Ordering::Relaxed);
    }
    /// Take (mean, max) since the last take.
    pub fn take() -> (f64, u64) {
        let sum = SUM.swap(0, Ordering::Relaxed);
        let count = COUNT.swap(0, Ordering::Relaxed);
        let max = MAX.swap(0, Ordering::Relaxed);
        (sum as f64 / count.max(1) as f64, max)
    }
}

/// Research instrument: while set, folds do not run (a crash between the
/// records and their fold).
pub static FOLD_PAUSED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Research instrument: forget every log this process holds WITHOUT folding
/// it, as a crash would: the unfolded records stay in storage only, and the
/// forgotten tails' folds stop.
pub fn crash_forget_all() {
    let tails: Vec<Arc<Tail>> = TAILS
        .lock()
        .expect("wal tails poisoned")
        .drain()
        .map(|(_, tail)| tail)
        .collect();
    for tail in tails {
        tail.dead.store(true, std::sync::atomic::Ordering::SeqCst);
        tail.wake.notify_one();
    }
}

/// Research instrument: write, straight to storage and outside this
/// process's log, the claim another process would append for `(root,
/// branch)`: this process's tip rows with the writer epoch raised by one, at
/// the version after the tip. The process's own next append then meets it.
pub async fn forge_foreign_claim_for_tests(root_uri: &str, branch: Option<&str>) -> Result<u64> {
    let tail = existing_tail(root_uri, branch)
        .ok_or_else(|| OmniError::manifest_internal("no log to forge a claim over"))?;
    let (version, tip, _) = tail.snapshot();
    let mut rows = (*tip).clone();
    let current = rows.writer_epoch.as_ref().map_or(0, |row| row.epoch);
    rows.writer_epoch = Some(crate::state::WriterEpochRow {
        epoch: current + 1,
        native: tail.native.clone(),
        attempt_id: ulid::Ulid::new().to_string(),
    });
    let bytes = encode(&rows, None)?;
    tail.store
        .put_if_absent(&tail.base.clone().join(record_name(version + 1)), bytes.into())
        .await
        .map_err(|error| OmniError::storage(error.into()))?;
    Ok(current + 1)
}

/// Research instrument: hide every log this process holds from opens until
/// the guard drops, as if the reader ran in another process.
pub fn hide_tails() -> HiddenTails {
    let hidden = std::mem::take(&mut *TAILS.lock().expect("wal tails poisoned"));
    HiddenTails(Some(hidden))
}

/// The guard of [`hide_tails`].
pub struct HiddenTails(Option<HashMap<TailKey, Arc<Tail>>>);

impl Drop for HiddenTails {
    fn drop(&mut self) {
        if let Some(hidden) = self.0.take() {
            TAILS.lock().expect("wal tails poisoned").extend(hidden);
        }
    }
}

/// Research instrument: wait until every log this process holds is folded.
pub async fn quiesce_all() -> Result<()> {
    let tails: Vec<Arc<Tail>> = TAILS
        .lock()
        .expect("wal tails poisoned")
        .values()
        .cloned()
        .collect();
    for tail in tails {
        tail.quiesce().await?;
    }
    Ok(())
}

/// Research instrument: forget every log this process holds (a restarted
/// process holds none), after folding them.
pub async fn forget_all() -> Result<()> {
    quiesce_all().await?;
    TAILS.lock().expect("wal tails poisoned").clear();
    Ok(())
}

/// Forget the log of `(root, branch)` (already folded): the next head-advancing
/// publication opens a new one over the latest `__manifest` version.
pub fn forget(root_uri: &str, branch: Option<&str>) {
    if let Some(tail) = TAILS
        .lock()
        .expect("wal tails poisoned")
        .remove(&key(root_uri, branch))
    {
        tail.dead.store(true, std::sync::atomic::Ordering::SeqCst);
        tail.wake.notify_one();
    }
}

/// The tail of the logical branch `(root, branch)` when this process has one.
pub fn existing_tail(root_uri: &str, branch: Option<&str>) -> Option<Arc<Tail>> {
    TAILS
        .lock()
        .expect("wal tails poisoned")
        .get(&key(root_uri, branch))
        .cloned()
}

async fn open_store(
    root_uri: &str,
    native: Option<&str>,
    session: &Arc<lance::session::Session>,
) -> Result<(Arc<ObjectStore>, Path)> {
    let uri = wal_uri(root_uri, native);
    let params = crate::storage::lance_store_params_for_session(&uri, session)?;
    ObjectStore::from_uri_and_params(session.store_registry(), &uri, &params)
        .await
        .map_err(OmniError::storage)
}

fn encode(rows: &ManifestRows, cold: Option<&LineageRegistry>) -> Result<Bytes> {
    // The stored (packed `record`) form, the one the publisher's row cache
    // decodes (`state::rows_of_batch`).
    let storage = crate::record::manifest_storage_schema(HashMap::new());
    let mut batches = vec![crate::record::compact_to_storage(&rows.to_batch()?, &storage)?];
    if let Some(cold) = cold {
        batches.push(crate::record::compact_to_storage(&cold.to_batch()?, &storage)?);
    }
    let schema = batches[0].schema();
    let mut out = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut out, &schema)
            .map_err(OmniError::arrow_internal)?;
        for batch in &batches {
            writer.write(batch).map_err(OmniError::arrow_internal)?;
        }
        writer.finish().map_err(OmniError::arrow_internal)?;
    }
    Ok(Bytes::from(out))
}

fn decode(bytes: &Bytes) -> Result<ManifestRows> {
    let reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes.as_ref()), None)
        .map_err(OmniError::arrow_internal)?;
    let batches = reader
        .collect::<std::result::Result<Vec<RecordBatch>, _>>()
        .map_err(OmniError::arrow_internal)?;
    let hot = batches
        .first()
        .ok_or_else(|| OmniError::manifest_internal("an empty WAL record"))?;
    crate::state::rows_of_batch(hot)
}

/// The records after `folded_version` this branch's log holds in storage, in
/// order: what a reader in another process, or a restarted writer, folds over
/// the latest `__manifest` version. One existence-or-content GET per record
/// plus one for the first absent version.
pub(crate) async fn read_storage_tail(
    root_uri: &str,
    native: Option<&str>,
    session: &Arc<lance::session::Session>,
    folded_version: u64,
) -> Result<Vec<(u64, ManifestRows)>> {
    let (store, base) = open_store(root_uri, native, session).await?;
    let mut records = Vec::new();
    let mut version = folded_version + 1;
    loop {
        let path = base.clone().join(record_name(version));
        match store.inner.get(&path).await {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(|e| OmniError::storage(e.into()))?;
                records.push((version, decode(&bytes)?));
                version += 1;
            }
            Err(object_store::Error::NotFound { .. }) => return Ok(records),
            Err(error) => return Err(OmniError::storage(error.into())),
        }
    }
}

/// The newest record version above `folded_version` storage holds, or
/// `folded_version` when there is none: one HEAD when the log is caught up;
/// otherwise one round of HEADs over the bounded tail (records are contiguous,
/// so the newest is the last of the leading run of present names).
pub(crate) async fn latest_record_version(
    root_uri: &str,
    native: Option<&str>,
    session: &Arc<lance::session::Session>,
    folded_version: u64,
) -> Result<u64> {
    let (store, base) = open_store(root_uri, native, session).await?;
    let exists = |version: u64| {
        let store = Arc::clone(&store);
        let path = base.clone().join(record_name(version));
        async move {
            match store.inner.head(&path).await {
                Ok(_) => Ok(true),
                Err(object_store::Error::NotFound { .. }) => Ok(false),
                Err(error) => Err(OmniError::storage(error.into())),
            }
        }
    };
    if !exists(folded_version + 1).await? {
        return Ok(folded_version);
    }
    // Exponential probe, then binary search (SlateDB's WAL tail probe):
    // records are contiguous, so `present` is a prefix of the versions.
    let mut present = folded_version + 1;
    let mut step = 1u64;
    let absent = loop {
        let candidate = present + step;
        if exists(candidate).await? {
            present = candidate;
            step *= 2;
        } else {
            break candidate;
        }
    };
    let (mut low, mut high) = (present, absent);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        if exists(middle).await? {
            low = middle;
        } else {
            high = middle;
        }
    }
    Ok(low)
}

/// The newest record above `folded_version` with its complete rows, or `None`
/// when the log holds none: what a reader with no log of the branch in its
/// process opens at. One HEAD when caught up; otherwise one round of HEADs and
/// one GET.
pub(crate) async fn read_latest_record(
    root_uri: &str,
    native: Option<&str>,
    session: &Arc<lance::session::Session>,
    folded_version: u64,
) -> Result<Option<(u64, ManifestRows)>> {
    let newest = latest_record_version(root_uri, native, session, folded_version).await?;
    if newest == folded_version {
        return Ok(None);
    }
    let (store, base) = open_store(root_uri, native, session).await?;
    let result = store
        .inner
        .get(&base.join(record_name(newest)))
        .await
        .map_err(|error| OmniError::storage(error.into()))?;
    let bytes = result.bytes().await.map_err(|e| OmniError::storage(e.into()))?;
    Ok(Some((newest, decode(&bytes)?)))
}

/// The tail of `(root, native)`, created over `dataset` (the latest folded
/// `__manifest` version) and its `rows` when this process has none; a new
/// tail first adopts the records storage holds above `dataset` (a previous
/// process's acknowledged, unfolded writes) and starts the fold.
pub(crate) async fn tail_for(
    root_uri: &str,
    branch: Option<&str>,
    native: Option<&str>,
    session: &Arc<lance::session::Session>,
    dataset: &Dataset,
    rows: &ManifestRows,
) -> Result<Arc<Tail>> {
    if let Some(tail) = existing_tail(root_uri, branch) {
        return Ok(tail);
    }
    let (store, base) = open_store(root_uri, native, session).await?;
    let recovered = read_storage_tail(root_uri, native, session, dataset.version().version).await?;
    let mut pending = VecDeque::new();
    let mut tip_version = dataset.version().version;
    let mut tip_rows = Arc::new(rows.clone());
    for (version, rows) in recovered {
        let rows = Arc::new(rows);
        tip_version = version;
        tip_rows = Arc::clone(&rows);
        pending.push_back(Pending {
            version,
            rows,
            cold: None,
        });
    }
    let tail = Arc::new(Tail {
        native: native.map(str::to_string),
        dead: std::sync::atomic::AtomicBool::new(false),
        store,
        base,
        state: Mutex::new(TailState {
            tip_version,
            tip_rows,
            folded: dataset.clone(),
            pending,
            failed: None,
        }),
        wake: tokio::sync::Notify::new(),
        progress: tokio::sync::Notify::new(),
    });
    let mut tails = TAILS.lock().expect("wal tails poisoned");
    if let Some(existing) = tails.get(&key(root_uri, branch)) {
        return Ok(Arc::clone(existing));
    }
    tails.insert(key(root_uri, branch), Arc::clone(&tail));
    drop(tails);
    tokio::spawn(fold_loop(Arc::clone(&tail)));
    tail.wake.notify_one();
    Ok(tail)
}

impl Tail {
    /// The native ref of the branch life this log belongs to (`None` on main).
    pub fn native(&self) -> Option<&str> {
        self.native.as_deref()
    }

    /// The latest version, its rows and the latest folded dataset.
    pub(crate) fn snapshot(&self) -> (u64, Arc<ManifestRows>, Dataset) {
        let state = self.state.lock().expect("wal tail poisoned");
        (
            state.tip_version,
            Arc::clone(&state.tip_rows),
            state.folded.clone(),
        )
    }

    /// The latest version and its rows, when the tail is above `version`.
    pub(crate) fn above(&self, version: u64) -> Option<(u64, Arc<ManifestRows>)> {
        let state = self.state.lock().expect("wal tail poisoned");
        (state.tip_version > version).then(|| (state.tip_version, Arc::clone(&state.tip_rows)))
    }

    /// Records not yet folded.
    pub fn unfolded(&self) -> usize {
        self.state.lock().expect("wal tail poisoned").pending.len()
    }

    async fn wait_for_room(&self) -> Result<()> {
        loop {
            {
                let state = self.state.lock().expect("wal tail poisoned");
                if let Some(failed) = &state.failed {
                    return Err(OmniError::manifest_internal(format!("WAL fold failed: {failed}")));
                }
                if state.pending.len() < max_unfolded() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Append the record of version `version` holding `rows` (and the `cold`
    /// registry rows its publication pushes): the commit point. Answers the
    /// latest folded dataset. `RowLevelCasContention` when another writer holds
    /// the version.
    pub(crate) async fn append(
        &self,
        version: u64,
        rows: &ManifestRows,
        cold: Option<&LineageRegistry>,
    ) -> Result<Dataset> {
        self.wait_for_room().await?;
        {
            let state = self.state.lock().expect("wal tail poisoned");
            if state.tip_version + 1 != version {
                return Err(OmniError::manifest_read_set_changed(
                    "wal_tip",
                    Some((version - 1).to_string()),
                    Some(state.tip_version.to_string()),
                ));
            }
        }
        let bytes = encode(rows, cold)?;
        let path = self.base.clone().join(record_name(version));
        if let Err(error) = self.store.put_if_absent(&path, bytes.clone().into()).await {
            // A held name is our own earlier copy whose answer was lost (equal
            // bytes), or another writer's record: a lower or equal writer
            // epoch is a previous owner's acknowledged write that landed
            // first, adopted into this tail so the next attempt builds above
            // it; a higher epoch fences this writer (the claim is a record,
            // so a takeover always occupies a slot an old owner reaches).
            let existing = match self.store.inner.get(&path).await {
                Ok(result) => result.bytes().await.map_err(|e| OmniError::storage(e.into()))?,
                Err(_) => return Err(OmniError::storage(error.into())),
            };
            if existing != bytes {
                let theirs = decode(&existing)?;
                let epoch_of = |rows: &ManifestRows| rows.writer_epoch.as_ref().map_or(0, |row| row.epoch);
                let (ours_epoch, their_epoch) = (epoch_of(rows), epoch_of(&theirs));
                if their_epoch > ours_epoch {
                    return Err(OmniError::manifest_writer_fenced(
                        self.native.as_deref().unwrap_or("main"),
                        Some(ours_epoch),
                        None,
                        their_epoch,
                    ));
                }
                let mut state = self.state.lock().expect("wal tail poisoned");
                if state.tip_version + 1 == version {
                    let theirs = Arc::new(theirs);
                    state.tip_version = version;
                    state.tip_rows = Arc::clone(&theirs);
                    state.pending.push_back(Pending {
                        version,
                        rows: theirs,
                        cold: None,
                    });
                    drop(state);
                    self.wake.notify_one();
                }
                return Err(OmniError::manifest_row_level_cas_contention(format!(
                    "WAL record {version} is held by another writer's record"
                )));
            }
        }
        let mut state = self.state.lock().expect("wal tail poisoned");
        let rows = Arc::new(rows.clone());
        state.tip_version = version;
        state.tip_rows = Arc::clone(&rows);
        state.pending.push_back(Pending {
            version,
            rows,
            cold: cold.cloned().map(Arc::new),
        });
        lag::record(state.pending.len() as u64);
        let folded = state.folded.clone();
        drop(state);
        self.wake.notify_one();
        Ok(folded)
    }

    /// Wait until every record is folded into `__manifest`.
    pub async fn quiesce(&self) -> Result<()> {
        loop {
            {
                let state = self.state.lock().expect("wal tail poisoned");
                if let Some(failed) = &state.failed {
                    return Err(OmniError::manifest_internal(format!("WAL fold failed: {failed}")));
                }
                if state.pending.is_empty() {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    }
}

async fn fold_loop(tail: Arc<Tail>) {
    let mut failures = 0u32;
    loop {
        if tail.dead.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        if FOLD_PAUSED.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(5)).await;
            continue;
        }
        let next = {
            let state = tail.state.lock().expect("wal tail poisoned");
            state
                .pending
                .front()
                .cloned()
                .map(|pending| (pending, state.folded.clone()))
        };
        let Some((pending, folded)) = next else {
            tail.wake.notified().await;
            continue;
        };
        let landed = match crate::commit::overwrite(folded.clone(), &pending.rows, pending.cold.as_deref()).await {
            Ok((dataset, _)) if dataset.version().version == pending.version => Ok(dataset),
            Ok((dataset, _)) => Err(format!(
                "folded record {} landed as __manifest version {}",
                pending.version,
                dataset.version().version
            )),
            // An earlier fold attempt of this record may have landed with its
            // answer lost: the version holds this record's head or another's.
            Err(error) => match folded.checkout_version(pending.version).await {
                Ok(existing) => match crate::state::read_manifest_rows(&existing).await {
                    Ok(rows) if rows.head == pending.rows.head => Ok(existing),
                    Ok(_) => Err(format!(
                        "__manifest version {} holds another commit than its record",
                        pending.version
                    )),
                    Err(_) => Err(error.to_string()),
                },
                Err(_) => Err(error.to_string()),
            },
        };
        match landed {
            Ok(dataset) => {
                failures = 0;
                let mut state = tail.state.lock().expect("wal tail poisoned");
                state.folded = dataset;
                state.pending.pop_front();
                drop(state);
                tail.progress.notify_waiters();
            }
            Err(error) => {
                failures += 1;
                if failures >= 20 {
                    tail.state.lock().expect("wal tail poisoned").failed = Some(error);
                    tail.progress.notify_waiters();
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10 * u64::from(failures))).await;
            }
        }
    }
}
