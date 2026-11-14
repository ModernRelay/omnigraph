//! Group commit for insert-only writes on one branch (research prototype,
//! `research/write-throughput`; design in the draft group-commit RFC).
//!
//! A write that only inserts by key, names no expected head and publishes on
//! its branch prepares as before (capture, execute, validate, stage), then
//! submits its staged effects here instead of taking the branch gate itself.
//! Submissions queue per (handle, branch, actor). The first writer to find no
//! batch in flight leads: it takes the branch's writer gates, captures the
//! current authority once, admits the queued entries, composes every table's
//! admitted inserts into one Lance transaction read on the table's current
//! pin, commits those detached, and publishes them in one `__manifest`
//! compare-and-swap as one graph commit. Every admitted entry is answered with
//! that commit; a refused entry gets `ReadSetChanged` and prepares again
//! through the existing loop. A leader leads only the batch holding its own
//! entry, then hands the lead to the next waiting entry, so batching is
//! natural (whatever queued while a publication was in flight), never timed.
//!
//! Admission (an entry captured at head `H0`, the batch forming at `Hc`):
//! - its branch incarnation and accepted schema are the batch's;
//! - `H0 == Hc`, or every commit in `(H0, Hc]` is one of this group's own
//!   batches (the effect log), whose inserted ids do not meet the entry's on
//!   any table the entry writes: an insert-only entry read only key absence
//!   on its own tables and endpoint existence, which inserts keep. Exact id
//!   sets decide when both sides kept them (`StagedWrite::inserted_ids`);
//!   otherwise Lance's key filters do, whose Bloom false positives refuse and
//!   never admit;
//! - no entry admitted earlier in this batch inserts an id it may share.
//!
//! The batch is conflict-serializable in admission order. A commit by any
//! other writer clears the effect log, so every entry captured before it is
//! refused and prepares again outside the gates. The compare-and-swap stays
//! the only cross-process fence.
//!
//! On a branch other than the handle's bound branch the group keeps the
//! coordinator it last published through (and installs it as the handle's
//! branch authority), so a batch there neither probes nor reopens the branch.
//! With `OMNIGRAPH_GROUP_COMMIT_PIPELINE=1` a batch composes and commits its
//! detached versions on the pins the in-flight batch will publish while that
//! batch publishes (`lead_pipelined`); its staging witness then names the
//! predecessor's parent head, which the collector's staging rule does not yet
//! accept (see the research report).
//!
//! Not production: a dropped leader future strands its batch (the server owns
//! its writes, an embedded caller may not), an in-doubt outcome is reported to
//! followers as an internal error carrying the leader's message and evidence,
//! the queue is unbounded, the ordinary path's failpoint seams are not crossed,
//! and an entry refused by a row-preserving publication (optimize, an index
//! build) is not carried over it.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Mutex;

use lance::dataset::write::merge_insert::inserted_rows::KeyExistenceFilter;
use tokio::sync::mpsc;

use crate::db::GraphCommit;
use crate::db::manifest::{ExpectedTableVersions, HistoryReleaseBytes, TableVersionExpectation};
use crate::db::omnigraph::{Omnigraph, WriteTxn};
use crate::error::{OmniError, Result};
use crate::exec::staging::GroupTableWrite;
use crate::storage_layer::StagedHandle;

/// Most entries one batch admits.
const MAX_BATCH_ENTRIES: usize = 512;
/// Batches the effect log remembers: the horizon an entry's capture may lag.
const EFFECT_LOG_BATCHES: usize = 128;

/// Whether group commit is on for this process (research switch).
pub(crate) fn enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT").is_ok_and(|value| value == "1")
}

/// Whether a publishing writer captures its bound branch with no probe
/// (research switch, off by default: see the note at its use).
pub(crate) fn writer_capture_skip_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_WRITER_CAPTURE_SKIP").is_ok_and(|value| value == "1")
}

/// One write's staged effects, submitted for a batch.
pub(crate) struct GroupEntry {
    pub(crate) txn: WriteTxn,
    pub(crate) writes: Vec<GroupTableWrite>,
    pub(crate) expected: ExpectedTableVersions,
    pub(crate) actor: Option<String>,
    pub(crate) history_release_bytes: HistoryReleaseBytes,
}

enum Signal {
    Done(Result<GraphCommit>),
    Lead,
}

struct Waiting {
    entry: GroupEntry,
    reply: mpsc::UnboundedSender<Signal>,
}

/// The keys inserted into one table: exact ids when every write kept them,
/// and the union of Lance's key filters, the fallback comparison.
#[derive(Clone)]
struct KeySet {
    filter: KeyExistenceFilter,
    ids: Option<HashSet<String>>,
}

impl KeySet {
    fn of(write: &GroupTableWrite) -> Self {
        Self {
            filter: write.filter.clone(),
            ids: write
                .ids
                .as_ref()
                .map(|ids| ids.iter().cloned().collect()),
        }
    }

    /// Whether `write` may insert a key of this set. Exact sets decide when
    /// both sides kept them; otherwise the Bloom filters do, whose false
    /// positives refuse and never admit.
    fn may_overlap(&self, write: &GroupTableWrite) -> bool {
        match (&self.ids, &write.ids) {
            (Some(mine), Some(theirs)) => theirs.iter().any(|id| mine.contains(id)),
            _ => crate::table_store::key_filters_may_overlap(&self.filter, &write.filter),
        }
    }

    /// Add `write`'s keys; `false` when the filters cannot be united.
    fn absorb(&mut self, write: &GroupTableWrite) -> bool {
        let Some(filter) = crate::table_store::union_key_filters(&self.filter, &write.filter)
        else {
            return false;
        };
        self.filter = filter;
        self.ids = match (self.ids.take(), &write.ids) {
            (Some(mut mine), Some(theirs)) => {
                mine.extend(theirs.iter().cloned());
                Some(mine)
            }
            _ => None,
        };
        true
    }
}

/// What one of this group's batches published: the head it built on, the
/// head it produced, and the keys of every table it inserted into.
#[derive(Clone)]
struct BatchEffect {
    parent: Option<String>,
    head: String,
    tables: HashMap<String, KeySet>,
}

#[derive(Default)]
struct Group {
    queue: VecDeque<Waiting>,
    leading: bool,
    log: VecDeque<BatchEffect>,
    /// Entries the last batch admitted: the cohort a closed loop of writers
    /// sends back once its answers arrive.
    last_batch: usize,
    /// Pipelining: the batch whose publication is in flight, which the next
    /// batch may compose on.
    in_flight: Option<std::sync::Arc<InFlight>>,
    /// For a branch other than the handle's bound branch: the coordinator
    /// this group last published through, whose view is that publication
    /// and whose publisher holds its acknowledged rows.
    coordinator: Option<crate::db::graph_coordinator::GraphCoordinator>,
}

/// A batch whose publication is in flight: the head it builds on, the keys it
/// inserts, the pins it will publish, and its outcome once known (`Some(head)`
/// published, `None` not).
struct InFlight {
    parent: Option<String>,
    tables: HashMap<String, KeySet>,
    pins: HashMap<String, crate::db::DatasetEntry>,
    done: tokio::sync::watch::Receiver<Option<Option<String>>>,
}

/// The head a pipelined batch builds on before its predecessor's commit id
/// exists: no capture can carry it.
const PENDING_HEAD: &str = "\u{0}pending";

/// Every group of one handle, keyed by branch and actor.
#[derive(Default)]
pub(crate) struct GroupCommits {
    groups: Mutex<HashMap<String, Group>>,
}

fn group_key(branch: Option<&str>, actor: Option<&str>) -> String {
    format!("{}\u{0}{}", branch.unwrap_or("main"), actor.unwrap_or(""))
}

fn moved(txn: &WriteTxn, current: &Option<String>) -> OmniError {
    OmniError::manifest_read_set_changed(
        format!("graph_head:{}", txn.branch.as_deref().unwrap_or("main")),
        txn.authority.graph_head.clone(),
        current.clone(),
    )
}

/// Rebuild an error for a follower: `ReadSetChanged` keeps its shape so the
/// follower prepares again; anything else is reported with its message.
fn follower_error(error: &OmniError, txn: &WriteTxn) -> OmniError {
    if error.is_read_set_changed() {
        return moved(txn, &None);
    }
    // Keep what the failure established about the publication, so a server
    // classifies every member of the batch as it would the batch's own
    // failure (`Unpublished` stays definite; anything else stays uncertain).
    let follower = OmniError::manifest_internal(format!("group commit failed: {error}"));
    match error.completion_evidence() {
        Some(evidence) => follower.with_completion_evidence(evidence),
        None => follower,
    }
}

/// What stage A of a pipelined batch hands stage B.
struct StagedBatch {
    admitted: Vec<Waiting>,
    updates: Vec<crate::db::DatasetUpdate>,
    detached: Vec<crate::storage_layer::SnapshotHandle>,
    expected: ExpectedTableVersions,
    keys: HashMap<String, KeySet>,
    /// The pin of every composed table this batch was committed on.
    assumed: HashMap<String, crate::db::DatasetEntry>,
    /// The in-flight batch this one composed on.
    predecessor: Option<std::sync::Arc<InFlight>>,
    branch: Option<String>,
    actor: Option<String>,
    history_release_bytes: HistoryReleaseBytes,
    /// The pins this batch will publish, for the batch after it.
    new_pins: HashMap<String, crate::db::DatasetEntry>,
}

/// Research switch: pipelined batches (`OMNIGRAPH_GROUP_COMMIT_PIPELINE=1`).
fn pipeline_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_PIPELINE").is_ok_and(|value| value == "1")
}

/// The outcome a leader hands back: every entry's answer and how the effect
/// log changes.
struct BatchOutcome {
    replies: Vec<(mpsc::UnboundedSender<Signal>, Result<GraphCommit>)>,
    log: LogChange,
}

enum LogChange {
    Keep,
    Clear,
    Push(BatchEffect),
}

impl Omnigraph {
    /// Submit one insert-only write's staged effects and wait for the batch
    /// that publishes them.
    pub(crate) async fn group_commit(&self, entry: GroupEntry) -> Result<GraphCommit> {
        let key = group_key(entry.txn.branch.as_deref(), entry.actor.as_deref());
        let (reply, mut signals) = mpsc::unbounded_channel();
        let lead_now = {
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.entry(key.clone()).or_default();
            group.queue.push_back(Waiting { entry, reply });
            if group.leading {
                false
            } else {
                group.leading = true;
                true
            }
        };
        if lead_now {
            self.lead(&key).await;
        }
        loop {
            match signals.recv().await {
                Some(Signal::Done(result)) => return result,
                Some(Signal::Lead) => self.lead(&key).await,
                None => {
                    return Err(OmniError::manifest_internal(
                        "a group commit leader went away without answering",
                    ));
                }
            }
        }
    }

    async fn lead(&self, key: &str) {
        if pipeline_enabled() {
            self.lead_pipelined(key).await;
        } else {
            self.lead_group(key).await;
        }
    }

    /// Hand the composing lead to the first queued entry whose writer is
    /// still waiting, or end it.
    fn hand_lead(group: &mut Group) {
        while let Some(next) = group.queue.front() {
            if next.reply.send(Signal::Lead).is_ok() {
                return;
            }
            group.queue.pop_front();
        }
        group.leading = false;
    }

    /// Pipelined lead (research): stage A, with no gate, captures the
    /// handle's view, admits the queued entries and composes and commits
    /// each table's inserts detached on the pins the in-flight batch will
    /// publish (or the current ones); stage B waits for that batch, checks
    /// that it published exactly the pins stage A assumed, takes the branch
    /// gates and publishes. The composing lead passes to the next batch as
    /// soon as this one enters stage B, so the next batch's detached commits
    /// overlap this batch's publication. A predecessor that did not publish
    /// refuses every entry of this batch before its publication: its
    /// detached commits stay unreachable (invariant 5).
    async fn lead_pipelined(&self, key: &str) {
        let (batch, log, in_flight) = {
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            let take = group.queue.len().min(MAX_BATCH_ENTRIES);
            let batch: Vec<Waiting> = group.queue.drain(..take).collect();
            (
                batch,
                group.log.iter().cloned().collect::<Vec<_>>(),
                group.in_flight.clone(),
            )
        };
        if batch.is_empty() {
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            Self::hand_lead(group);
            return;
        }
        let started = std::time::Instant::now();
        let mut replies = Vec::new();
        let staged = self.stage_batch(batch, &log, in_flight, &mut replies).await;
        let staged_at = started.elapsed();
        let Some(staged) = staged else {
            // Nothing admitted (or stage A failed): answer and pass the lead.
            for (reply, result) in replies {
                let _ = reply.send(Signal::Done(result));
            }
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            Self::hand_lead(group);
            return;
        };
        let StagedBatch {
            admitted,
            updates,
            detached,
            expected,
            keys,
            assumed,
            predecessor,
            branch,
            actor,
            history_release_bytes,
            new_pins,
        } = staged;

        // Stage B: the predecessor's outcome first.
        let mut refusal: Option<OmniError> = None;
        let mut expected_head: Option<Option<String>> = None;
        if let Some(prev) = &predecessor {
            let mut done = prev.done.clone();
            let outcome = loop {
                if let Some(outcome) = done.borrow().clone() {
                    break outcome;
                }
                if done.changed().await.is_err() {
                    break None;
                }
            };
            match outcome {
                Some(head) => expected_head = Some(Some(head)),
                None => {
                    refusal = Some(OmniError::manifest_read_set_changed(
                        "group_predecessor_not_published".to_string(),
                        prev.parent.clone(),
                        None,
                    ))
                }
            }
        }
        let gates = match refusal {
            Some(_) => None,
            None => match self
                .write_queue()
                .acquire_branch_write_gates(branch.as_deref())
                .await
            {
                Ok(gates) => Some(gates),
                Err(error) => {
                    refusal = Some(error);
                    None
                }
            },
        };
        let mut publish_coordinator = None;
        let current = match refusal {
            Some(_) => None,
            None => {
                let log_now: Vec<BatchEffect> = {
                    let groups = self.group_commits().groups.lock().unwrap();
                    groups
                        .get(key)
                        .map(|group| group.log.iter().cloned().collect())
                        .unwrap_or_default()
                };
                match self.batch_capture(key, &branch, &log_now, &admitted).await {
                    Ok((txn, coordinator)) => {
                        publish_coordinator = coordinator;
                        Some(txn)
                    }
                    Err(error) => {
                        refusal = Some(error);
                        None
                    }
                }
            }
        };
        // The published state must be exactly the one stage A composed on.
        if refusal.is_none()
            && let Some(current) = &current
        {
            let head_ok = expected_head
                .as_ref()
                .is_none_or(|head| &current.authority.graph_head == head);
            let pins_ok = assumed.iter().all(|(table, pin)| {
                current.base.dataset(table).is_some_and(|entry| {
                    entry.identity == pin.identity
                        && entry.published_dataset_version == pin.published_dataset_version
                        && entry.version_metadata.staged_version()
                            == pin.version_metadata.staged_version()
                })
            }) && expected.values().all(|expectation| {
                current
                    .base
                    .dataset(&expectation.table_key)
                    .is_some_and(|entry| {
                        entry.published_dataset_version == expectation.table_version
                    })
            });
            if !head_ok || !pins_ok {
                refusal = Some(OmniError::manifest_read_set_changed(
                    "group_pipeline_base_moved".to_string(),
                    None,
                    current.authority.graph_head.clone(),
                ));
            }
        }
        if let Some(error) = refusal {
            if debug_enabled() {
                eprintln!("[group] pipelined batch of {} refused: {error}", admitted.len());
            }
            drop(gates);
            for waiting in admitted {
                let error = follower_error(&error, &waiting.entry.txn);
                replies.push((waiting.reply, Err(error)));
            }
            for (reply, result) in replies {
                let _ = reply.send(Signal::Done(result));
            }
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            group.log.clear();
            Self::hand_lead(group);
            return;
        }
        let current = current.expect("captured above");
        let head = current.authority.graph_head.clone();
        // Become the in-flight batch and pass the composing lead on.
        let (done_tx, done_rx) = tokio::sync::watch::channel(None);
        let mine = std::sync::Arc::new(InFlight {
            parent: head.clone(),
            tables: keys.clone(),
            pins: new_pins,
            done: done_rx,
        });
        {
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            group.in_flight = Some(std::sync::Arc::clone(&mine));
            Self::hand_lead(group);
        }
        let published = self
            .publish_staged(
                &current,
                updates,
                detached,
                &expected,
                actor.as_deref(),
                history_release_bytes,
                publish_coordinator.as_mut(),
            )
            .await;
        if publish_coordinator.is_some() {
            self.keep_coordinator(key, published.as_ref().ok().and(publish_coordinator))
                .await;
        }
        drop(gates);
        let _ = done_tx.send(Some(
            published
                .as_ref()
                .ok()
                .map(|commit| commit.graph_commit_id.clone()),
        ));
        if timing_enabled() {
            eprintln!(
                "[group-timing] pipelined entries {} stage-a {:.1} total {:.1} ms (pipelined on predecessor {})",
                admitted.len(),
                staged_at.as_secs_f64() * 1000.0,
                started.elapsed().as_secs_f64() * 1000.0,
                predecessor.is_some(),
            );
        }
        let admitted_count = admitted.len();
        match &published {
            Ok(commit) => {
                for waiting in admitted {
                    replies.push((waiting.reply, Ok(commit.clone())));
                }
            }
            Err(error) => {
                if debug_enabled() {
                    eprintln!("[group] pipelined batch of {admitted_count} failed: {error}");
                }
                for waiting in admitted {
                    let error = follower_error(error, &waiting.entry.txn);
                    replies.push((waiting.reply, Err(error)));
                }
            }
        }
        {
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            if group
                .in_flight
                .as_ref()
                .is_some_and(|flight| std::sync::Arc::ptr_eq(flight, &mine))
            {
                group.in_flight = None;
            }
            match &published {
                Ok(commit) => {
                    group.log.push_back(BatchEffect {
                        parent: head,
                        head: commit.graph_commit_id.clone(),
                        tables: keys,
                    });
                    while group.log.len() > EFFECT_LOG_BATCHES {
                        group.log.pop_front();
                    }
                    group.last_batch = admitted_count;
                }
                Err(_) => group.log.clear(),
            }
        }
        for (reply, result) in replies {
            let _ = reply.send(Signal::Done(result));
        }
    }

    /// Lead one batch: everything queued now, the leader's own entry among
    /// it. Answers every entry, applies the log change, then hands the lead
    /// to the next queued entry or ends the group's lead.
    async fn lead_group(&self, key: &str) {
        // Linger: when far fewer entries are queued than the last batch
        // admitted, the rest of that cohort is still preparing its next
        // write; wait a bounded moment so it joins this batch instead of
        // the one after (a lone entry would otherwise publish alone and the
        // cohort queue behind it). Never at low load: a last batch of one,
        // or a queue at least half its size, leads at once.
        let linger = linger_budget();
        if !linger.is_zero() {
            let deadline = std::time::Instant::now() + linger;
            loop {
                let (queued, cohort) = {
                    let groups = self.group_commits().groups.lock().unwrap();
                    let group = groups.get(key).expect("a leading group exists");
                    (group.queue.len(), group.last_batch)
                };
                if cohort <= 1 || queued * 2 >= cohort || std::time::Instant::now() >= deadline {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        }
        let (batch, log) = {
            let mut groups = self.group_commits().groups.lock().unwrap();
            let group = groups.get_mut(key).expect("a leading group exists");
            let take = group.queue.len().min(MAX_BATCH_ENTRIES);
            let batch: Vec<Waiting> = group.queue.drain(..take).collect();
            (batch, group.log.iter().cloned().collect::<Vec<_>>())
        };
        let outcome = if batch.is_empty() {
            BatchOutcome {
                replies: Vec::new(),
                log: LogChange::Keep,
            }
        } else {
            self.run_batch(batch, &log).await
        };
        let admitted = outcome
            .replies
            .iter()
            .filter(|(_, result)| result.is_ok())
            .count();
        for (reply, result) in outcome.replies {
            let _ = reply.send(Signal::Done(result));
        }
        let mut groups = self.group_commits().groups.lock().unwrap();
        let group = groups.get_mut(key).expect("a leading group exists");
        group.last_batch = admitted;
        match outcome.log {
            LogChange::Keep => {}
            LogChange::Clear => group.log.clear(),
            LogChange::Push(effect) => {
                group.log.push_back(effect);
                while group.log.len() > EFFECT_LOG_BATCHES {
                    group.log.pop_front();
                }
            }
        }
        // Hand the lead to the first queued entry whose writer is still
        // waiting; a writer that went away is dropped from the queue.
        while let Some(next) = group.queue.front() {
            if next.reply.send(Signal::Lead).is_ok() {
                return;
            }
            group.queue.pop_front();
        }
        group.leading = false;
    }

    /// Capture a batch's authority. On the handle's bound branch, from the
    /// installed view, with no probe when it is this group's own last
    /// publication. On another branch, from the coordinator this group last
    /// published through under the same rule, else from a fresh open; the
    /// coordinator to publish through comes back beside the capture.
    async fn batch_capture(
        &self,
        key: &str,
        branch: &Option<String>,
        log: &[BatchEffect],
        entries: &[Waiting],
    ) -> Result<(
        WriteTxn,
        Option<crate::db::graph_coordinator::GraphCoordinator>,
    )> {
        let (bound, installed) = self.installed_head();
        // The view this group last published is the batch's capture only
        // while every entry was captured at a head the group's log explains:
        // an entry captured at a head the log does not know saw a
        // publication this view did not, so the leader probes.
        let explained = entries.iter().all(|waiting| {
            let head = &waiting.entry.txn.authority.graph_head;
            head.as_ref().is_some_and(|head| {
                log.iter()
                    .any(|effect| &effect.head == head || effect.parent.as_ref() == Some(head))
            })
        });
        let last = log.last().map(|effect| effect.head.clone()).filter(|_| explained);
        if bound == *branch {
            let own_view =
                capture_skip_enabled() && installed.is_some() && last.as_ref() == installed.as_ref();
            let txn = self.open_write_txn_probed(branch.as_deref(), !own_view).await?;
            return Ok((txn, None));
        }
        let held = {
            let groups = self.group_commits().groups.lock().unwrap();
            groups
                .get(key)
                .and_then(|group| group.coordinator.clone())
        };
        if debug_enabled() {
            eprintln!(
                "[group] capture {:?}: held head {:?}, log head {:?}",
                branch,
                held.as_ref().map(|coordinator| coordinator.exact_graph_head()),
                last
            );
        }
        let coordinator = match held {
            Some(coordinator)
                if capture_skip_enabled()
                    && last.is_some()
                    && coordinator.exact_graph_head() == last =>
            {
                coordinator
            }
            _ => self.open_coordinator_for_branch(branch.as_deref()).await?,
        };
        let txn = self.write_txn_from_coordinator(&coordinator).await?;
        Ok((txn, Some(coordinator)))
    }

    /// Research (group commit): a writer's capture of a branch other than the
    /// handle's bound branch from the coordinator its group last published
    /// through, with no probe; `None` when the group holds none.
    pub(crate) async fn group_view_txn(
        &self,
        branch: Option<&str>,
        actor: Option<&str>,
    ) -> Option<Result<WriteTxn>> {
        let key = group_key(branch, actor);
        let held = {
            let groups = self.group_commits().groups.lock().unwrap();
            groups.get(&key).and_then(|group| group.coordinator.clone())
        }?;
        Some(self.write_txn_from_coordinator(&held).await)
    }

    async fn keep_coordinator(
        &self,
        key: &str,
        coordinator: Option<crate::db::graph_coordinator::GraphCoordinator>,
    ) {
        // One per-branch writer state: the handle's branch authority cache,
        // which writers capture through, gets the view this group published.
        if let Some(coordinator) = &coordinator {
            self.install_branch_authority(coordinator).await;
        }
        let mut groups = self.group_commits().groups.lock().unwrap();
        if let Some(group) = groups.get_mut(key) {
            group.coordinator = coordinator;
        }
    }

    /// Stage A of a pipelined batch: capture the handle's view (with no probe
    /// when it is this group's own last publication), admit the batch
    /// against the effect log and the in-flight batch's pending effect, and
    /// compose and commit each table's inserts detached on the pin the
    /// in-flight batch will publish, or the current one. `None` when nothing
    /// was admitted; every refused entry's answer goes to `replies`.
    async fn stage_batch(
        &self,
        batch: Vec<Waiting>,
        log: &[BatchEffect],
        in_flight: Option<std::sync::Arc<InFlight>>,
        replies: &mut Vec<(mpsc::UnboundedSender<Signal>, Result<GraphCommit>)>,
    ) -> Option<StagedBatch> {
        let branch = batch[0].entry.txn.branch.clone();
        let mut log = log.to_vec();
        let mut in_flight = in_flight;
        let mut attempts = 0;
        let key = group_key(branch.as_deref(), batch[0].entry.actor.as_deref());
        let (current, predecessor) = loop {
            attempts += 1;
            let current = match self.batch_capture(&key, &branch, &log, &batch).await {
                Ok((txn, _)) => txn,
                Err(error) => {
                    for waiting in batch {
                        let error = follower_error(&error, &waiting.entry.txn);
                        replies.push((waiting.reply, Err(error)));
                    }
                    return None;
                }
            };
            match in_flight.take() {
                Some(flight) if flight.parent == current.authority.graph_head => {
                    break (current, Some(flight));
                }
                Some(flight) if attempts < 3 => {
                    // The in-flight batch published (or failed) between the
                    // read of the group and this capture: wait for it, then
                    // read the log it left and capture again.
                    let mut done = flight.done.clone();
                    while done.borrow().is_none() {
                        if done.changed().await.is_err() {
                            break;
                        }
                    }
                    let _ = done;
                    let groups = self.group_commits().groups.lock().unwrap();
                    if let Some(group) = groups.get(&key) {
                        log = group.log.iter().cloned().collect();
                        in_flight = group.in_flight.clone();
                    }
                    continue;
                }
                _ => break (current, None),
            }
        };
        let head = current.authority.graph_head.clone();
        let log_reaches_head = log
            .last()
            .is_some_and(|effect| Some(&effect.head) == head.as_ref());
        let mut admission_log: Vec<BatchEffect> = if log_reaches_head {
            log
        } else {
            Vec::new()
        };
        let batch_head = match &predecessor {
            Some(flight) => {
                admission_log.push(BatchEffect {
                    parent: flight.parent.clone(),
                    head: PENDING_HEAD.to_string(),
                    tables: flight.tables.clone(),
                });
                Some(PENDING_HEAD.to_string())
            }
            None => head.clone(),
        };

        let mut admitted: Vec<Waiting> = Vec::new();
        let mut keys: HashMap<String, KeySet> = HashMap::new();
        for waiting in batch {
            match admit(&waiting.entry, &current, &batch_head, &admission_log, &keys) {
                Ok(()) => {
                    for write in &waiting.entry.writes {
                        match keys.get_mut(&write.table_key) {
                            None => {
                                keys.insert(write.table_key.clone(), KeySet::of(write));
                            }
                            Some(existing) => {
                                let _ = existing.absorb(write);
                            }
                        }
                    }
                    admitted.push(waiting);
                }
                Err(error) => replies.push((waiting.reply, Err(error))),
            }
        }
        if admitted.is_empty() {
            return None;
        }
        let base_pin = |table: &str| -> Option<crate::db::DatasetEntry> {
            predecessor
                .as_ref()
                .and_then(|flight| flight.pins.get(table).cloned())
                .or_else(|| current.base.dataset(table).cloned())
        };
        let mut by_table: BTreeMap<String, (String, Vec<StagedHandle>)> = BTreeMap::new();
        let mut expected: ExpectedTableVersions = HashMap::new();
        let mut missing = None;
        for waiting in &admitted {
            for write in &waiting.entry.writes {
                by_table
                    .entry(write.table_key.clone())
                    .or_insert_with(|| (write.full_path.clone(), Vec::new()))
                    .1
                    .push(write.staged.clone());
            }
            for (identity, expectation) in &waiting.entry.expected {
                match base_pin(&expectation.table_key) {
                    Some(pin) => {
                        expected.insert(
                            *identity,
                            TableVersionExpectation {
                                table_key: expectation.table_key.clone(),
                                table_version: pin.published_dataset_version,
                                native_ref: expectation.native_ref.clone(),
                            },
                        );
                    }
                    None => missing = Some(expectation.table_key.clone()),
                }
            }
        }
        let fail = |admitted: Vec<Waiting>,
                    error: OmniError,
                    replies: &mut Vec<(mpsc::UnboundedSender<Signal>, Result<GraphCommit>)>| {
            for waiting in admitted {
                let error = follower_error(&error, &waiting.entry.txn);
                replies.push((waiting.reply, Err(error)));
            }
            None
        };
        if let Some(table) = missing {
            return fail(
                admitted,
                OmniError::manifest_read_set_changed(format!("table:{table}"), None, None),
                replies,
            );
        }
        let witness = match current.authority.staging_witness() {
            Ok(witness) => witness,
            Err(error) => return fail(admitted, error, replies),
        };
        let table_fork_owner = match current.branch.as_deref() {
            None => None,
            Some(_) => current.base.native_branch(),
        };
        let witness = &witness;
        let current_ref = &current;
        let base_pin = &base_pin;
        let commits = by_table.into_iter().map(|(table_key, (full_path, writes))| async move {
            let pin = base_pin(&table_key).ok_or_else(|| {
                OmniError::manifest_read_set_changed(format!("table:{table_key}"), None, None)
            })?;
            let base = self.open_pinned_for_write(&full_path, &pin).await?;
            let composed = StagedHandle::compose_insert_only(&base, writes)?;
            let declared_full_text = crate::db::omnigraph::table_ops::declared_full_text_columns(
                &current_ref.catalog,
                &table_key,
            );
            let (detached, identity) = self
                .storage()
                .commit_staged_detached(base, composed, witness, &declared_full_text)
                .await?;
            let state = self.storage().table_state(&full_path, &detached).await?;
            let target = pin.published_dataset_version + 1;
            let version_metadata = state
                .version_metadata
                .with_table_fork_owner(table_fork_owner)
                .with_staged(state.version, identity.uuid.clone())
                .with_last_linear_version(pin.version_metadata.last_linear_version());
            self.read_caches()
                .handles
                .insert(
                    &pin.dataset_path,
                    pin.native_dataset_branch.as_deref(),
                    target,
                    Some(state.version),
                    version_metadata.e_tag(),
                    detached.dataset().clone(),
                )
                .await;
            let update = crate::db::DatasetUpdate {
                identity: pin.identity,
                type_key: table_key.clone(),
                published_dataset_version: target,
                native_dataset_branch: pin.native_dataset_branch.clone(),
                entity_count: state.row_count,
                version_metadata,
            };
            Ok::<_, OmniError>((table_key, pin, update, detached))
        });
        let committed = match futures::future::try_join_all(commits).await {
            Ok(committed) => committed,
            Err(error) => return fail(admitted, error, replies),
        };
        let mut updates = Vec::with_capacity(committed.len());
        let mut detached = Vec::with_capacity(committed.len());
        let mut assumed = HashMap::new();
        let mut new_pins = HashMap::new();
        for (table_key, pin, update, handle) in committed {
            new_pins.insert(
                table_key.clone(),
                crate::db::DatasetEntry {
                    identity: update.identity,
                    type_key: update.type_key.clone(),
                    dataset_path: pin.dataset_path.clone(),
                    published_dataset_version: update.published_dataset_version,
                    native_dataset_branch: update.native_dataset_branch.clone(),
                    entity_count: update.entity_count,
                    version_metadata: update.version_metadata.clone(),
                    manifest_version: 0,
                },
            );
            assumed.insert(table_key, pin);
            updates.push(update);
            detached.push(handle);
        }
        let first = &admitted[0].entry;
        Some(StagedBatch {
            actor: first.actor.clone(),
            history_release_bytes: first.history_release_bytes,
            admitted,
            updates,
            detached,
            expected,
            keys,
            assumed,
            predecessor,
            branch,
            new_pins,
        })
    }

    /// Stage B's publication: one compare-and-swap of every pin, with the
    /// new versions' index metadata read meanwhile.
    async fn publish_staged(
        &self,
        current: &WriteTxn,
        updates: Vec<crate::db::DatasetUpdate>,
        detached: Vec<crate::storage_layer::SnapshotHandle>,
        expected: &ExpectedTableVersions,
        actor: Option<&str>,
        history_release_bytes: HistoryReleaseBytes,
        coordinator: Option<&mut crate::db::graph_coordinator::GraphCoordinator>,
    ) -> Result<GraphCommit> {
        let lineage_intent = self
            .new_lineage_intent_for_branch(current.branch.as_deref(), actor, history_release_bytes)
            .await
            .map_err(OmniError::unpublished)?;
        let publish = async {
            match coordinator {
                Some(coordinator) => {
                    self.commit_updates_with_coordinator(
                        coordinator,
                        &updates,
                        expected,
                        current,
                        lineage_intent,
                    )
                    .await
                }
                None => {
                    self.commit_updates_on_branch_with_expected(
                        current.branch.as_deref(),
                        &updates,
                        expected,
                        actor,
                        current,
                        lineage_intent,
                        crate::db::omnigraph::Publication::Publish,
                    )
                    .await
                }
            }
        };
        let warm = async {
            if index_warm_enabled() {
                futures::future::join_all(detached.iter().map(|handle| async move {
                    use lance::index::DatasetIndexExt as _;
                    let _ = handle.dataset().load_indices().await;
                }))
                .await;
            }
        };
        let (published, ()) = futures::join!(publish, warm);
        published
    }

    async fn run_batch(&self, batch: Vec<Waiting>, log: &[BatchEffect]) -> BatchOutcome {
        let fail_all = |batch: Vec<Waiting>, error: OmniError, log: LogChange| BatchOutcome {
            replies: batch
                .into_iter()
                .map(|waiting| {
                    let error = follower_error(&error, &waiting.entry.txn);
                    (waiting.reply, Err(error))
                })
                .collect(),
            log,
        };
        let branch = batch[0].entry.txn.branch.clone();
        let started = std::time::Instant::now();
        let _gates = match self
            .write_queue()
            .acquire_branch_write_gates(branch.as_deref())
            .await
        {
            Ok(gates) => gates,
            Err(error) => return fail_all(batch, error, LogChange::Keep),
        };
        let gated = started.elapsed();
        // One capture of the current authority for the whole batch. When the
        // handle's installed view is this group's own last publication, it is
        // the capture, with no probe: a commit by another writer since makes
        // the publication's compare-and-swap refuse the batch, as a stale
        // view does on every write path, and its detached commits stay
        // unreachable (invariant 5).
        let key = group_key(branch.as_deref(), batch[0].entry.actor.as_deref());
        let (current, mut publish_coordinator) =
            match self.batch_capture(&key, &branch, log, &batch).await {
                Ok(captured) => captured,
                Err(error) => return fail_all(batch, error, LogChange::Clear),
            };
        let own_view = true;
        let captured = started.elapsed();
        let head = current.authority.graph_head.clone();
        let log_reaches_head = log
            .last()
            .is_some_and(|effect| Some(&effect.head) == head.as_ref());
        if debug_enabled() && !log.is_empty() && !log_reaches_head {
            eprintln!(
                "[group] log head {:?} is not the branch head {:?}",
                log.last().map(|effect| &effect.head),
                head
            );
        }
        let log = if log_reaches_head { log } else { &[][..] };

        // Admission, in queue order.
        let mut admitted: Vec<Waiting> = Vec::new();
        let mut replies = Vec::new();
        let mut batch_filters: HashMap<String, KeySet> = HashMap::new();
        for waiting in batch {
            match admit(
                &waiting.entry,
                &current,
                &current.authority.graph_head,
                log,
                &batch_filters,
            ) {
                Ok(()) => {
                    for write in &waiting.entry.writes {
                        match batch_filters.get_mut(&write.table_key) {
                            None => {
                                batch_filters.insert(write.table_key.clone(), KeySet::of(write));
                            }
                            Some(existing) => {
                                // admit() refused incomparable filters.
                                let _ = existing.absorb(write);
                            }
                        }
                    }
                    admitted.push(waiting);
                }
                Err(error) => replies.push((waiting.reply, Err(error))),
            }
        }
        if admitted.is_empty() {
            return BatchOutcome {
                replies,
                log: if log_reaches_head {
                    LogChange::Keep
                } else {
                    LogChange::Clear
                },
            };
        }

        let admitted_count = admitted.len();
        let published = self
            .publish_batch(&current, &admitted, publish_coordinator.as_mut())
            .await;
        if publish_coordinator.is_some() {
            self.keep_coordinator(&key, published.as_ref().ok().and(publish_coordinator))
                .await;
        }
        if timing_enabled() {
            eprintln!(
                "[group-timing] entries {admitted_count} gates {:.1} capture {:.1} commit+publish {:.1} ms (own view {own_view})",
                gated.as_secs_f64() * 1000.0,
                (captured - gated).as_secs_f64() * 1000.0,
                (started.elapsed() - captured).as_secs_f64() * 1000.0,
            );
        }
        match published {
            Ok(commit) => {
                let effect = BatchEffect {
                    parent: head,
                    head: commit.graph_commit_id.clone(),
                    tables: batch_filters,
                };
                for waiting in admitted {
                    replies.push((waiting.reply, Ok(commit.clone())));
                }
                BatchOutcome {
                    replies,
                    log: LogChange::Push(effect),
                }
            }
            Err(error) => {
                if debug_enabled() {
                    eprintln!("[group] batch of {} failed: {error}", admitted.len());
                }
                for waiting in admitted {
                    let error = follower_error(&error, &waiting.entry.txn);
                    replies.push((waiting.reply, Err(error)));
                }
                BatchOutcome {
                    replies,
                    log: LogChange::Clear,
                }
            }
        }
    }

    /// Compose each table's admitted inserts on its current pin, commit
    /// them detached, and publish every pin in one compare-and-swap.
    async fn publish_batch(
        &self,
        current: &WriteTxn,
        admitted: &[Waiting],
        coordinator: Option<&mut crate::db::graph_coordinator::GraphCoordinator>,
    ) -> Result<GraphCommit> {
        // Each table's effects, in admission order.
        let mut by_table: BTreeMap<String, (String, Vec<StagedHandle>)> = BTreeMap::new();
        let mut expected: ExpectedTableVersions = HashMap::new();
        for waiting in admitted {
            for write in &waiting.entry.writes {
                by_table
                    .entry(write.table_key.clone())
                    .or_insert_with(|| (write.full_path.clone(), Vec::new()))
                    .1
                    .push(write.staged.clone());
            }
            // Every touched table expects its pin at the batch's head: the
            // admission proved the entry's reads hold there.
            for (identity, expectation) in &waiting.entry.expected {
                let entry = current.base.dataset(&expectation.table_key).ok_or_else(|| {
                    OmniError::manifest_read_set_changed(
                        format!("table:{}", expectation.table_key),
                        Some(expectation.table_version.to_string()),
                        None,
                    )
                })?;
                expected.insert(
                    *identity,
                    TableVersionExpectation {
                        table_key: expectation.table_key.clone(),
                        table_version: entry.published_dataset_version,
                        native_ref: expectation.native_ref.clone(),
                    },
                );
            }
        }
        let witness = current.authority.staging_witness()?;
        let witness = &witness;
        let table_fork_owner = match current.branch.as_deref() {
            None => None,
            Some(branch) => Some(current.base.native_branch().ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "branch '{branch}' has no captured native fork owner"
                ))
            })?),
        };
        let commits = by_table.into_iter().map(|(table_key, (full_path, writes))| async move {
            let entry = current.base.dataset(&table_key).ok_or_else(|| {
                OmniError::manifest_read_set_changed(format!("table:{table_key}"), None, None)
            })?;
            let base = self.open_pinned_for_write(&full_path, entry).await?;
            let composed = StagedHandle::compose_insert_only(&base, writes)?;
            let declared_full_text =
                crate::db::omnigraph::table_ops::declared_full_text_columns(
                    &current.catalog,
                    &table_key,
                );
            let (detached, identity) = self
                .storage()
                .commit_staged_detached(base, composed, witness, &declared_full_text)
                .await?;
            let state = self.storage().table_state(&full_path, &detached).await?;
            let target = entry.published_dataset_version + 1;
            let version_metadata = state
                .version_metadata
                .with_table_fork_owner(table_fork_owner)
                .with_staged(state.version, identity.uuid.clone())
                .with_last_linear_version(entry.version_metadata.last_linear_version());
            self.read_caches()
                .handles
                .insert(
                    &entry.dataset_path,
                    entry.native_dataset_branch.as_deref(),
                    target,
                    Some(state.version),
                    version_metadata.e_tag(),
                    detached.dataset().clone(),
                )
                .await;
            Ok::<_, OmniError>((
                crate::db::DatasetUpdate {
                    identity: entry.identity,
                    type_key: table_key.clone(),
                    published_dataset_version: target,
                    native_dataset_branch: entry.native_dataset_branch.clone(),
                    entity_count: state.row_count,
                    version_metadata,
                },
                detached,
            ))
        });
        let commit_started = std::time::Instant::now();
        let committed = futures::future::try_join_all(commits)
            .await
            .map_err(OmniError::unpublished)?;
        let (updates, detached): (Vec<_>, Vec<_>) = committed.into_iter().unzip();
        if timing_enabled() {
            eprintln!(
                "[group-timing]   detached commits {:.1} ms",
                commit_started.elapsed().as_secs_f64() * 1000.0
            );
        }
        let first = &admitted[0].entry;
        let lineage_intent = self
            .new_lineage_intent_for_branch(
                current.branch.as_deref(),
                first.actor.as_deref(),
                first.history_release_bytes,
            )
            .await
            .map_err(OmniError::unpublished)?;
        let publish = async {
            match coordinator {
                Some(coordinator) => {
                    self.commit_updates_with_coordinator(
                        coordinator,
                        &updates,
                        &expected,
                        current,
                        lineage_intent,
                    )
                    .await
                }
                None => {
                    self.commit_updates_on_branch_with_expected(
                        current.branch.as_deref(),
                        &updates,
                        &expected,
                        first.actor.as_deref(),
                        current,
                        lineage_intent,
                        crate::db::omnigraph::Publication::Publish,
                    )
                    .await
                }
            }
        };
        // While the publication is in flight, read each new version's index
        // metadata into the shared Lance session: the next writer's
        // validation plans against that version and would otherwise read
        // the index section itself, one request before its staging.
        let warm = async {
            if index_warm_enabled() {
                futures::future::join_all(detached.iter().map(|handle| async move {
                    use lance::index::DatasetIndexExt as _;
                    let _ = handle.dataset().load_indices().await;
                }))
                .await;
            }
        };
        let (published, ()) = futures::join!(publish, warm);
        published
    }
}

/// Whether `entry` may publish in a batch forming at `current`'s head, given
/// this group's effect log since and the batch's keys so far.
/// Research switch: warm each new version's index metadata during the
/// publication (on by default with group commit).
fn index_warm_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_INDEX_WARM").map_or(true, |value| value != "0")
}

/// Research switch: the most a leader lingers for the rest of the last
/// batch's cohort (`OMNIGRAPH_GROUP_COMMIT_LINGER_MS`, default 0 = off).
fn linger_budget() -> std::time::Duration {
    std::time::Duration::from_millis(
        std::env::var("OMNIGRAPH_GROUP_COMMIT_LINGER_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(0),
    )
}

fn timing_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_TIMING").is_ok_and(|value| value == "1")
}

/// Whether a leader may capture from its own last publication without a
/// probe (research switch, on by default with group commit).
fn capture_skip_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_CAPTURE_SKIP").map_or(true, |value| value != "0")
}

fn debug_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_DEBUG").is_ok_and(|value| value == "1")
}

fn admit(
    entry: &GroupEntry,
    current: &WriteTxn,
    batch_head: &Option<String>,
    log: &[BatchEffect],
    batch_filters: &HashMap<String, KeySet>,
) -> Result<()> {
    let result = admit_inner(entry, current, batch_head, log, batch_filters);
    if debug_enabled()
        && let Err(reason) = &result
    {
        eprintln!(
            "[group] refuse entry at {:?} (batch head {:?}, log {}): {reason}",
            entry.txn.authority.graph_head,
            current.authority.graph_head,
            log.len()
        );
    }
    result
}

fn admit_inner(
    entry: &GroupEntry,
    current: &WriteTxn,
    batch_head: &Option<String>,
    log: &[BatchEffect],
    batch_filters: &HashMap<String, KeySet>,
) -> Result<()> {
    let authority = &entry.txn.authority;
    let now = &current.authority;
    if authority.branch_identifier != now.branch_identifier
        || authority.schema_ir_hash != now.schema_ir_hash
        || authority.schema_identity_domain != now.schema_identity_domain
        || authority.schema_identity_version != now.schema_identity_version
    {
        return Err(moved(&entry.txn, &now.graph_head));
    }
    for write in &entry.writes {
        let registered = current
            .base
            .dataset(&write.table_key)
            .is_some_and(|registered| registered.identity == write.identity);
        if !registered {
            return Err(moved(&entry.txn, &now.graph_head));
        }
    }
    if &authority.graph_head != batch_head {
        // Walk this group's batches back from the current head to the
        // entry's capture; every one of them must be ours and insert keys
        // disjoint from the entry's on every table the entry writes.
        let mut reached = false;
        for effect in log.iter().rev() {
            for write in &entry.writes {
                if let Some(keys) = effect.tables.get(&write.table_key)
                    && keys.may_overlap(write)
                {
                    return Err(OmniError::manifest_read_set_changed(
                        format!("group_key_overlap_since_capture:{}", write.table_key),
                        authority.graph_head.clone(),
                        now.graph_head.clone(),
                    ));
                }
            }
            if effect.parent == authority.graph_head {
                reached = true;
                break;
            }
        }
        if !reached {
            return Err(OmniError::manifest_read_set_changed(
                format!("group_log_does_not_reach_capture:{}", log.len()),
                authority.graph_head.clone(),
                now.graph_head.clone(),
            ));
        }
    }
    for write in &entry.writes {
        if let Some(keys) = batch_filters.get(&write.table_key) {
            if keys.may_overlap(write)
                || crate::table_store::union_key_filters(&keys.filter, &write.filter).is_none()
            {
                return Err(OmniError::manifest_read_set_changed(
                    format!("group_key_overlap_in_batch:{}", write.table_key),
                    authority.graph_head.clone(),
                    now.graph_head.clone(),
                ));
            }
        }
    }
    Ok(())
}
