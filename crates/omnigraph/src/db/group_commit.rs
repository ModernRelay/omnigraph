//! Group commit for insert-only writes on one branch (research prototype,
//! `research/write-throughput`; design in the group-commit RFC of PR #785).
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
//!   batches (the effect log), whose per-table key filters do not overlap the
//!   entry's on any table the entry writes: an insert-only entry read only
//!   key absence on its own tables (Lance's key filters decide it; a Bloom
//!   false positive refuses, never admits) and endpoint existence, which
//!   inserts keep;
//! - no entry admitted earlier in this batch inserts a key it may share.
//!
//! The batch is conflict-serializable in admission order. A commit by any
//! other writer clears the effect log, so every entry captured before it is
//! refused. The compare-and-swap stays the only cross-process fence.
//!
//! Not production: a dropped leader future strands its batch (the server owns
//! its writes, an embedded caller may not), an in-doubt outcome is reported to
//! followers as an internal error carrying the leader's message, and the queue
//! is unbounded.

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
}

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
        moved(txn, &None)
    } else {
        OmniError::manifest_internal(format!("group commit failed: {error}"))
    }
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
            self.lead_group(&key).await;
        }
        loop {
            match signals.recv().await {
                Some(Signal::Done(result)) => return result,
                Some(Signal::Lead) => self.lead_group(&key).await,
                None => {
                    return Err(OmniError::manifest_internal(
                        "a group commit leader went away without answering",
                    ));
                }
            }
        }
    }

    /// Lead one batch: everything queued now, the leader's own entry among
    /// it. Answers every entry, applies the log change, then hands the lead
    /// to the next queued entry or ends the group's lead.
    async fn lead_group(&self, key: &str) {
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
        for (reply, result) in outcome.replies {
            let _ = reply.send(Signal::Done(result));
        }
        let mut groups = self.group_commits().groups.lock().unwrap();
        let group = groups.get_mut(key).expect("a leading group exists");
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
        let _gates = match self
            .write_queue()
            .acquire_branch_write_gates(branch.as_deref())
            .await
        {
            Ok(gates) => gates,
            Err(error) => return fail_all(batch, error, LogChange::Keep),
        };
        // One capture of the current authority for the whole batch.
        let current = match self.open_write_txn(branch.as_deref()).await {
            Ok(txn) => txn,
            Err(error) => return fail_all(batch, error, LogChange::Clear),
        };
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
            match admit(&waiting.entry, &current, log, &batch_filters) {
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

        match self.publish_batch(&current, &admitted).await {
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
    async fn publish_batch(&self, current: &WriteTxn, admitted: &[Waiting]) -> Result<GraphCommit> {
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
            Ok::<_, OmniError>(crate::db::DatasetUpdate {
                identity: entry.identity,
                type_key: table_key.clone(),
                published_dataset_version: target,
                native_dataset_branch: entry.native_dataset_branch.clone(),
                entity_count: state.row_count,
                version_metadata,
            })
        });
        let updates = futures::future::try_join_all(commits)
            .await
            .map_err(OmniError::unpublished)?;
        let first = &admitted[0].entry;
        let lineage_intent = self
            .new_lineage_intent_for_branch(
                current.branch.as_deref(),
                first.actor.as_deref(),
                first.history_release_bytes,
            )
            .await
            .map_err(OmniError::unpublished)?;
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

/// Whether `entry` may publish in a batch forming at `current`'s head, given
/// this group's effect log since and the batch's keys so far.
fn debug_enabled() -> bool {
    std::env::var("OMNIGRAPH_GROUP_COMMIT_DEBUG").is_ok_and(|value| value == "1")
}

fn admit(
    entry: &GroupEntry,
    current: &WriteTxn,
    log: &[BatchEffect],
    batch_filters: &HashMap<String, KeySet>,
) -> Result<()> {
    let result = admit_inner(entry, current, log, batch_filters);
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
    if authority.graph_head != now.graph_head {
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
