//! The cluster lock's lease: heartbeats, self-fencing and takeover.
//!
//! A held version-3 lock (`state_lock`) carries `lease_seconds`; only a
//! server's lock on S3 is leased. Its owner renews it by creating
//! `__cluster/lease/<lock_id>/beat-<seq>.json`, put-if-absent, every third
//! of a lease. A successor that finds the lock held watches those beats: a
//! new beat means the owner is alive, and the successor refuses. When no
//! beat appears for a lease and a half of the successor's own monotonic
//! time, the successor claims the lock once by creating
//! `__cluster/lease/<lock_id>/superseded.json` put-if-absent, then replaces
//! `lock.json` with its own record by if-match on the version it first read.
//!
//! The owner fences itself first. It measures from the time it first sent
//! its last acknowledged beat, and when two thirds of a lease pass without a
//! newer one, or a claim names its lock, it stops: the holder closes
//! admission and storage mutation and exits, and never resumes, because a
//! successor may already act. The gap between the owner's two thirds of a
//! lease and the successor's lease and a half covers clock-rate drift and
//! scheduling delay; the protocol needs no synchronized clocks.
//!
//! The first beat races the same deadline, counted from the send of the
//! lock's own write, because a successor may count silence from the moment
//! the record is visible: a holder whose first beat is not acknowledged in
//! time gets no lease and must not serve. A fence closes the holder's
//! storage scope before it is reported, so the holder sends no storage
//! mutation after a successor may claim, whatever its shutdown grace.
//!
//! Heartbeats, the watch and the claim go through the root outside the
//! owner's storage scope, on a client with the backend's own retries. A beat
//! is a key only its owner creates, under a lock id no other process uses, so
//! a request that lands late only keeps a successor waiting. A beat that is
//! already present when its owner sends it is that owner's own earlier
//! request, which landed after the beat's first send: the owner counts it as
//! acknowledged from that first send. A beat that gets no answer is sent
//! again soon, under the same key, until the owner's deadline.
//!
//! Only operations S3 provides are used: create-if-absent, list, read,
//! delete of this owner's own keys, and the if-match replacement of
//! `lock.json`. Local roots have no conditional update, so a successor could
//! not replace a record without racing its owner's release; their locks
//! carry no lease, and a dead owner's lock waits for an operator's exact-ID
//! unlock as before.
//!
//! The lease coordinates liveness. It is not a fence for the requests a dead
//! owner sent before it fenced: those are fresh keys, which stay unreachable,
//! conditional publications, which apply at most once against the version
//! the successor also writes, or the audited mutable keys whose late landing
//! is harmless.

use std::sync::Arc;
use std::time::Duration;

use omnigraph_storage::{StorageAdapter, StorageIoScope};
use tokio::sync::{Notify, watch};
use tokio::time::Instant;

/// The lease a new lock records unless `OMNIGRAPH_CLUSTER_LEASE_SECONDS`
/// sets another.
pub(crate) const DEFAULT_LEASE_SECONDS: u32 = 30;
pub(crate) const LEASE_SECONDS_ENV: &str = "OMNIGRAPH_CLUSTER_LEASE_SECONDS";
const MIN_LEASE_SECONDS: u32 = 2;
const MAX_LEASE_SECONDS: u32 = 600;

/// Whether a recorded lease is one this protocol accepts, so a corrupt record
/// cannot hold a successor off indefinitely.
pub(crate) fn valid_lease_seconds(seconds: u32) -> bool {
    (MIN_LEASE_SECONDS..=MAX_LEASE_SECONDS).contains(&seconds)
}

/// The lease a new lock records: the environment's value within
/// `2..=600` seconds, else [`DEFAULT_LEASE_SECONDS`].
pub(crate) fn configured_lease_seconds() -> u32 {
    std::env::var(LEASE_SECONDS_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|seconds| valid_lease_seconds(*seconds))
        .unwrap_or(DEFAULT_LEASE_SECONDS)
}

fn lease(seconds: u32) -> Duration {
    Duration::from_secs(u64::from(seconds))
}

/// How often the owner beats: a third of the lease.
fn beat_interval(seconds: u32) -> Duration {
    lease(seconds) / 3
}

/// The owner fences when this long has passed since the first send of its
/// last acknowledged beat: two beat intervals.
fn owner_deadline(seconds: u32) -> Duration {
    lease(seconds) * 2 / 3
}

/// How soon a beat that got no answer is sent again: a quarter of a beat
/// interval, so several attempts fit before the owner's deadline.
fn reissue_pause(seconds: u32) -> Duration {
    beat_interval(seconds) / 4
}

/// A successor takes over after this much beat silence: a lease and a half.
fn successor_silence(seconds: u32) -> Duration {
    lease(seconds) * 3 / 2
}

pub fn lease_dir(root: &str, lock_id: &str) -> String {
    format!("{}/__cluster/lease/{lock_id}", root.trim_end_matches('/'))
}

fn beat_uri(dir: &str, seq: u64) -> String {
    format!("{dir}/beat-{seq:020}.json")
}

fn claim_uri(dir: &str) -> String {
    format!("{dir}/superseded.json")
}

/// The newest beat a listing of `dir` holds.
async fn newest_beat(
    adapter: &dyn StorageAdapter,
    dir: &str,
) -> omnigraph_storage::Result<Option<u64>> {
    Ok(adapter
        .list_dir(dir)
        .await?
        .iter()
        .filter_map(|uri| {
            uri.rsplit('/')
                .next()?
                .strip_prefix("beat-")?
                .strip_suffix(".json")?
                .parse::<u64>()
                .ok()
        })
        .max())
}

/// Why an owner fenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaseFenced {
    /// Two beats in a row went unacknowledged: a successor may take over.
    Lapsed,
    /// A successor claimed the lock.
    Superseded,
}

/// A holder's watch on its lease, detached from the admission it came from.
#[derive(Debug, Clone)]
pub struct LeaseFence(pub(crate) Option<watch::Receiver<Option<LeaseFenced>>>);

impl LeaseFence {
    /// Resolves once the lease fenced: a successor may take over, so the
    /// holder must stop writing and must not release. Never resolves for an
    /// unleased lock.
    pub async fn fenced(mut self) -> LeaseFenced {
        if let Some(fenced) = self.0.as_mut() {
            loop {
                if let Some(fence) = *fenced.borrow_and_update() {
                    return fence;
                }
                if fenced.changed().await.is_err() {
                    break;
                }
            }
        }
        std::future::pending().await
    }
}

/// The owner side: a task that beats until stopped or fenced.
#[derive(Debug)]
pub struct LeaseKeeper {
    fenced: watch::Receiver<Option<LeaseFenced>>,
    stop: Arc<Notify>,
    task: Option<tokio::task::JoinHandle<()>>,
    dir: String,
}

impl LeaseKeeper {
    /// Start beating for `lock_id`, whose lock record was written by a
    /// request sent at `lock_sent`.
    ///
    /// A successor may start counting silence as soon as the record is
    /// visible, which is no earlier than `lock_sent`, so the first beat races
    /// the owner's deadline counted from there, like every later one. When
    /// it is not acknowledged in time, or a claim already names the lock,
    /// this returns the fence and no keeper: the caller must not serve.
    ///
    /// When the lease fences, `scope` closes before the fence is published,
    /// so the holder sends no storage mutation after a successor may act.
    pub async fn start(
        adapter: Arc<dyn StorageAdapter>,
        root: &str,
        lock_id: &str,
        seconds: u32,
        lock_sent: Instant,
        scope: Option<StorageIoScope>,
    ) -> Result<Self, LeaseFenced> {
        let dir = lease_dir(root, lock_id);
        let (sender, fenced) = watch::channel(None);
        let stop = Arc::new(Notify::new());
        let mut beat = Beat {
            adapter,
            dir: dir.clone(),
            seq: 0,
            first_sent: None,
            acknowledged: lock_sent,
            answered: true,
        };
        let first = tokio::select! {
            biased;
            fence = beat.first(seconds) => fence,
            () = tokio::time::sleep_until(lock_sent + owner_deadline(seconds)) => {
                Some(LeaseFenced::Lapsed)
            }
        };
        if let Some(fence) = first {
            fence_holder(scope.as_ref(), fence);
            return Err(fence);
        }
        let task = tokio::spawn({
            let stop = Arc::clone(&stop);
            async move {
                let fence = loop {
                    // The deadline is a timer of its own: a beat request that
                    // hangs must not postpone the owner's fence. A beat that
                    // completes at the same moment is counted first.
                    let deadline = beat.acknowledged + owner_deadline(seconds);
                    let pause = if beat.answered {
                        beat_interval(seconds)
                    } else {
                        reissue_pause(seconds)
                    };
                    let next = async {
                        tokio::time::sleep(pause).await;
                        beat.once().await
                    };
                    tokio::select! {
                        biased;
                        () = stop.notified() => return,
                        fence = next => if let Some(fence) = fence {
                            break fence;
                        },
                        () = tokio::time::sleep_until(deadline) => break LeaseFenced::Lapsed,
                    }
                };
                fence_holder(scope.as_ref(), fence);
                let _ = sender.send(Some(fence));
            }
        });
        Ok(Self {
            fenced,
            stop,
            task: Some(task),
            dir,
        })
    }

    /// Resolves once the lease fenced; never resolves while it holds.
    pub fn fenced(&self) -> watch::Receiver<Option<LeaseFenced>> {
        self.fenced.clone()
    }

    pub(crate) fn dir(&self) -> &str {
        &self.dir
    }

    /// Stop beating, for a clean release. A successor still sees the lock
    /// until the release writes it.
    pub(crate) async fn stop(mut self) {
        self.stop.notify_one();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for LeaseKeeper {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct Beat {
    adapter: Arc<dyn StorageAdapter>,
    dir: String,
    seq: u64,
    /// When the first request for beat `seq` was sent.
    first_sent: Option<Instant>,
    /// The first send time of the last acknowledged beat.
    acknowledged: Instant,
    /// Whether the last request got an answer; one that did not is sent
    /// again sooner.
    answered: bool,
}

/// Stop the holder's storage mutations, then report the fence. A successor
/// may claim the lock five sixths of a lease after the owner's deadline;
/// closing the scope here, not when the process exits, keeps every
/// mutation this holder sends before that.
fn fence_holder(scope: Option<&StorageIoScope>, fence: LeaseFenced) {
    if let Some(scope) = scope {
        scope.close();
    }
    tracing::error!(
        ?fence,
        "cluster admission lease fenced; storage mutation closed, this process must stop"
    );
}

impl Beat {
    /// Send beat 0 until it is acknowledged, or a claim names the lock.
    async fn first(&mut self, seconds: u32) -> Option<LeaseFenced> {
        loop {
            if let Some(fence) = self.once().await {
                return Some(fence);
            }
            if self.answered {
                return None;
            }
            tokio::time::sleep(reissue_pause(seconds)).await;
        }
    }

    /// Check for a claim, send beat `seq` (again, if an earlier request for
    /// it got no answer), and report a fence.
    async fn once(&mut self) -> Option<LeaseFenced> {
        if let Ok(Some(_)) = self
            .adapter
            .read_text_if_exists(&claim_uri(&self.dir))
            .await
        {
            return Some(LeaseFenced::Superseded);
        }
        let sent = *self.first_sent.get_or_insert_with(Instant::now);
        let payload = format!("{{\"seq\":{}}}", self.seq);
        match self
            .adapter
            .write_text_if_absent(&beat_uri(&self.dir, self.seq), &payload)
            .await
        {
            // `false`: the beat is already present, which only this owner's
            // own earlier request for it can have done, no earlier than its
            // first send. Either way it is visible now and was sent no
            // earlier than `sent`.
            Ok(_) => {
                self.acknowledged = sent;
                self.answered = true;
                if let Some(stale) = self.seq.checked_sub(2) {
                    let _ = self.adapter.delete(&beat_uri(&self.dir, stale)).await;
                }
                self.seq += 1;
                self.first_sent = None;
            }
            Err(_) => self.answered = false,
        }
        None
    }
}

/// What a successor found when it watched a held version-3 lock.
#[derive(Debug, PartialEq, Eq)]
pub enum Watch {
    /// A beat appeared, or another successor claimed the lock first.
    Alive,
    /// This successor holds the claim: it may replace `lock.json`.
    Claimed,
}

/// Watch `lock_id`'s beats for a lease and a half of silence, then claim the
/// lock. `poll` is how often the beat listing is read.
pub async fn watch_and_claim(
    adapter: &dyn StorageAdapter,
    root: &str,
    lock_id: &str,
    seconds: u32,
    successor_lock_id: &str,
) -> omnigraph_storage::Result<Watch> {
    let dir = lease_dir(root, lock_id);
    let claim = claim_uri(&dir);
    let poll = (lease(seconds) / 6).max(Duration::from_millis(100));
    let mut newest = newest_beat(adapter, &dir).await?;
    let mut since = Instant::now();
    loop {
        if adapter.read_text_if_exists(&claim).await?.is_some() {
            return Ok(Watch::Alive);
        }
        if since.elapsed() >= successor_silence(seconds) {
            break;
        }
        tokio::time::sleep(poll).await;
        let current = newest_beat(adapter, &dir).await?;
        if current != newest {
            if newest.is_some() {
                return Ok(Watch::Alive);
            }
            // The first beat seen restarts the silence: the owner may have
            // been between its lock and its first beat.
            newest = current;
            since = Instant::now();
        }
    }
    let payload = serde_json::json!({
        "successor_lock_id": successor_lock_id,
        "newest_beat": newest,
    })
    .to_string();
    if adapter.write_text_if_absent(&claim, &payload).await? {
        return Ok(Watch::Claimed);
    }
    // A claim is already present. A retry finds this successor's own claim
    // when an earlier attempt landed unacknowledged; any other is another
    // successor's.
    let own = adapter
        .read_text_if_exists(&claim)
        .await?
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok())
        .is_some_and(|claim| claim["successor_lock_id"] == successor_lock_id);
    Ok(if own { Watch::Claimed } else { Watch::Alive })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use omnigraph_storage::{
        ListDirBounds, ObjectStorageAdapter, StorageError, StorageFailure, StorageFailureKind,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    const ROOT: &str = "cluster";
    const SECONDS: u32 = 30;

    /// An in-memory store that can stop acknowledging beats, the way a
    /// partitioned owner's requests stop coming back; fail some beats
    /// without applying them; lose the answer to beats it applied; leave
    /// beats unanswered for good; and answer a claim it applied as already
    /// present, the way a retried create whose first attempt landed is
    /// answered.
    #[derive(Debug)]
    struct Beats {
        inner: Arc<dyn StorageAdapter>,
        refuse: AtomicBool,
        failures: AtomicUsize,
        lost_answers: AtomicUsize,
        hang: AtomicBool,
        claim_retried: AtomicBool,
    }

    impl Beats {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                inner: Arc::new(ObjectStorageAdapter::in_memory()),
                refuse: AtomicBool::new(false),
                failures: AtomicUsize::new(0),
                lost_answers: AtomicUsize::new(0),
                hang: AtomicBool::new(false),
                claim_retried: AtomicBool::new(false),
            })
        }
    }

    fn take_one(counter: &AtomicUsize) -> bool {
        counter
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
    }

    async fn start(store: &Arc<Beats>, lock_id: &str) -> LeaseKeeper {
        LeaseKeeper::start(store.clone(), ROOT, lock_id, SECONDS, Instant::now(), None)
            .await
            .unwrap()
    }

    #[async_trait]
    impl StorageAdapter for Beats {
        async fn read_text(&self, uri: &str) -> omnigraph_storage::Result<String> {
            self.inner.read_text(uri).await
        }
        async fn read_text_if_exists(
            &self,
            uri: &str,
        ) -> omnigraph_storage::Result<Option<String>> {
            self.inner.read_text_if_exists(uri).await
        }
        async fn read_text_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> omnigraph_storage::Result<Option<String>> {
            self.inner.read_text_if_exists_bounded(uri, max_bytes).await
        }
        async fn read_bytes_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> omnigraph_storage::Result<Option<Vec<u8>>> {
            self.inner
                .read_bytes_if_exists_bounded(uri, max_bytes)
                .await
        }
        async fn write_text(&self, uri: &str, contents: &str) -> omnigraph_storage::Result<()> {
            self.inner.write_text(uri, contents).await
        }
        async fn write_bytes(&self, uri: &str, contents: &[u8]) -> omnigraph_storage::Result<()> {
            self.inner.write_bytes(uri, contents).await
        }
        async fn write_text_if_absent(
            &self,
            uri: &str,
            contents: &str,
        ) -> omnigraph_storage::Result<bool> {
            let unanswered = || {
                StorageError::Backend(StorageFailure::new(
                    StorageFailureKind::Transient,
                    "beat request unanswered",
                ))
            };
            let beat = uri.contains("/beat-");
            if beat && self.hang.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
            if beat && (self.refuse.load(Ordering::SeqCst) || take_one(&self.failures)) {
                return Err(unanswered());
            }
            let created = self.inner.write_text_if_absent(uri, contents).await?;
            if created && beat && take_one(&self.lost_answers) {
                return Err(unanswered());
            }
            if uri.ends_with("/superseded.json") && self.claim_retried.load(Ordering::SeqCst) {
                return Ok(false);
            }
            Ok(created)
        }
        async fn exists(&self, uri: &str) -> omnigraph_storage::Result<bool> {
            self.inner.exists(uri).await
        }
        async fn rename_text(&self, from: &str, to: &str) -> omnigraph_storage::Result<()> {
            self.inner.rename_text(from, to).await
        }
        async fn delete(&self, uri: &str) -> omnigraph_storage::Result<()> {
            self.inner.delete(uri).await
        }
        async fn list_dir(&self, dir_uri: &str) -> omnigraph_storage::Result<Vec<String>> {
            self.inner.list_dir(dir_uri).await
        }
        async fn list_dir_bounded(
            &self,
            dir_uri: &str,
            matching_suffix: &str,
            bounds: ListDirBounds,
        ) -> omnigraph_storage::Result<Vec<String>> {
            self.inner
                .list_dir_bounded(dir_uri, matching_suffix, bounds)
                .await
        }
        async fn read_text_versioned(
            &self,
            uri: &str,
        ) -> omnigraph_storage::Result<(String, String)> {
            self.inner.read_text_versioned(uri).await
        }
        async fn read_text_versioned_if_exists_bounded(
            &self,
            uri: &str,
            max_bytes: u64,
        ) -> omnigraph_storage::Result<Option<(String, String)>> {
            self.inner
                .read_text_versioned_if_exists_bounded(uri, max_bytes)
                .await
        }
        async fn write_text_if_match(
            &self,
            uri: &str,
            contents: &str,
            expected_version: &str,
        ) -> omnigraph_storage::Result<Option<String>> {
            self.inner
                .write_text_if_match(uri, contents, expected_version)
                .await
        }
        async fn delete_prefix(&self, prefix_uri: &str) -> omnigraph_storage::Result<()> {
            self.inner.delete_prefix(prefix_uri).await
        }
    }

    async fn fenced_within(keeper: &LeaseKeeper, limit: Duration) -> Option<LeaseFenced> {
        tokio::time::timeout(limit, LeaseFence(Some(keeper.fenced())).fenced())
            .await
            .ok()
    }

    /// A live owner keeps a successor out: its beats keep appearing, and the
    /// successor refuses without claiming the lock.
    #[tokio::test(start_paused = true)]
    async fn a_successor_refuses_while_the_owner_beats_issue_1067() {
        let store = Beats::new();
        let keeper = start(&store, "owner").await;
        let watched = watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "successor")
            .await
            .unwrap();
        assert_eq!(watched, Watch::Alive);
        assert!(
            store
                .read_text_if_exists(&claim_uri(&lease_dir(ROOT, "owner")))
                .await
                .unwrap()
                .is_none(),
            "a refusing successor claims nothing"
        );
        assert_eq!(fenced_within(&keeper, Duration::from_secs(60)).await, None);
    }

    /// A dead owner's beats stop. A successor claims the lock after a lease
    /// and a half of silence, never sooner, and only one of two successors
    /// wins the claim.
    #[tokio::test(start_paused = true)]
    async fn a_successor_claims_a_silent_owner_after_a_lease_and_a_half_issue_1067() {
        let store = Beats::new();
        let keeper = start(&store, "owner").await;
        drop(keeper);
        let started = Instant::now();
        let (first, second) = tokio::join!(
            watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "first"),
            watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "second"),
        );
        assert!(started.elapsed() >= successor_silence(SECONDS));
        let mut outcomes = [first.unwrap(), second.unwrap()];
        outcomes.sort_by_key(|watch| *watch == Watch::Claimed);
        assert_eq!(outcomes, [Watch::Alive, Watch::Claimed]);
    }

    /// An owner whose beats go unacknowledged fences itself after two missed
    /// beats, well before any successor may take over.
    #[tokio::test(start_paused = true)]
    async fn an_owner_fences_itself_when_its_beats_lapse_issue_1067() {
        let store = Beats::new();
        let started = Instant::now();
        let keeper = start(&store, "owner").await;
        store.refuse.store(true, Ordering::SeqCst);
        assert_eq!(
            fenced_within(&keeper, lease(SECONDS)).await,
            Some(LeaseFenced::Lapsed)
        );
        assert!(started.elapsed() <= owner_deadline(SECONDS) + beat_interval(SECONDS));
        assert!(started.elapsed() < successor_silence(SECONDS));
    }

    /// A beat the store refused is sent again soon, under the same key, until
    /// the deadline. A failed heartbeat, or several in a row within the
    /// deadline, never fences a live owner.
    #[tokio::test(start_paused = true)]
    async fn one_failed_heartbeat_does_not_fence_a_live_owner_issue_1067() {
        let store = Beats::new();
        let keeper = start(&store, "owner").await;
        store.failures.store(1, Ordering::SeqCst);
        assert_eq!(fenced_within(&keeper, lease(SECONDS) * 2).await, None);
        assert_eq!(store.failures.load(Ordering::SeqCst), 0);
        // Three failures in a row still leave a re-issue inside the deadline.
        store.failures.store(3, Ordering::SeqCst);
        assert_eq!(fenced_within(&keeper, lease(SECONDS) * 2).await, None);
        assert_eq!(store.failures.load(Ordering::SeqCst), 0);
    }

    /// A beat the store applied but whose answer was lost is the owner's own:
    /// the owner sends it again soon, finds it present, and counts it from
    /// its first send. Lost answers never fence a live owner, and a
    /// successor keeps seeing its beats.
    #[tokio::test(start_paused = true)]
    async fn a_landed_beat_whose_ack_was_lost_refreshes_the_owner_issue_1067() {
        let store = Beats::new();
        let keeper = start(&store, "owner").await;
        store.lost_answers.store(6, Ordering::SeqCst);
        assert_eq!(
            fenced_within(&keeper, lease(SECONDS) * 4).await,
            None,
            "an applied beat renews the lease even when its answer is lost"
        );
        assert_eq!(store.lost_answers.load(Ordering::SeqCst), 0);
        assert_eq!(
            watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "successor")
                .await
                .unwrap(),
            Watch::Alive
        );
    }

    /// A successor whose claim landed while its answer was lost finds the
    /// claim on its retry and recognizes it as its own; another successor's
    /// claim is not.
    #[tokio::test(start_paused = true)]
    async fn a_successor_recognizes_its_own_claim_issue_1067() {
        let store = Beats::new();
        drop(start(&store, "owner").await);
        store.claim_retried.store(true, Ordering::SeqCst);
        assert_eq!(
            watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "first")
                .await
                .unwrap(),
            Watch::Claimed
        );
        assert_eq!(
            watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "second")
                .await
                .unwrap(),
            Watch::Alive
        );
    }

    /// The lock is visible before the first beat lands, and a successor may
    /// count silence from then. A first beat that never answers fences the
    /// owner at its deadline, counted from the lock's write: `start` returns
    /// the fence and no keeper, before a successor can claim, and the
    /// holder's storage scope is closed.
    #[tokio::test(start_paused = true)]
    async fn a_hung_first_beat_fences_before_a_successor_can_claim_issue_1067() {
        let store = Beats::new();
        store.hang.store(true, Ordering::SeqCst);
        let scope = StorageIoScope::new();
        let lock_sent = Instant::now();
        let (owner, successor) = tokio::join!(
            async {
                let result = LeaseKeeper::start(
                    store.clone(),
                    ROOT,
                    "owner",
                    SECONDS,
                    lock_sent,
                    Some(scope.clone()),
                )
                .await;
                (result.map(|_| ()), lock_sent.elapsed())
            },
            async {
                let watched =
                    watch_and_claim(store.as_ref(), ROOT, "owner", SECONDS, "successor").await;
                (watched.unwrap(), lock_sent.elapsed())
            },
        );
        assert_eq!(owner.0, Err(LeaseFenced::Lapsed));
        assert!(owner.1 <= owner_deadline(SECONDS), "{:?}", owner.1);
        assert_eq!(successor.0, Watch::Claimed);
        assert!(successor.1 >= successor_silence(SECONDS));
        let dir = tempfile::tempdir().unwrap();
        let fenced = ObjectStorageAdapter::local_scoped(scope);
        assert!(
            fenced
                .write_text(&format!("{}/after-fence", dir.path().display()), "x")
                .await
                .is_err(),
            "a fenced holder's storage scope refuses mutations"
        );
    }

    /// A claim on its lock fences the owner at its next beat.
    #[tokio::test(start_paused = true)]
    async fn a_claim_fences_the_owner_issue_1067() {
        let store = Beats::new();
        let keeper = start(&store, "owner").await;
        assert!(
            store
                .write_text_if_absent(&claim_uri(&lease_dir(ROOT, "owner")), "{}")
                .await
                .unwrap()
        );
        assert_eq!(
            fenced_within(&keeper, beat_interval(SECONDS) * 2).await,
            Some(LeaseFenced::Superseded)
        );
    }

    #[test]
    fn the_owner_fences_before_any_successor_may_take_over() {
        for seconds in [MIN_LEASE_SECONDS, DEFAULT_LEASE_SECONDS, MAX_LEASE_SECONDS] {
            // The owner's last chance to notice is its deadline plus one
            // beat interval; a successor waits a lease and a half after the
            // beat it saw, which was sent no later than it was seen.
            assert!(
                owner_deadline(seconds) + beat_interval(seconds) < successor_silence(seconds),
                "{seconds}"
            );
        }
    }
}
