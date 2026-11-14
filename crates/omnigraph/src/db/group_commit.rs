//! WAL research prototype: group commit at the branch publisher.
//!
//! Not a production path. Enabled per process by `OMNIGRAPH_GROUP_COMMIT=1`
//! (read once). Several insertion-only mutations on one branch publish with
//! one `__manifest` publication (or, with `OMNIGRAPH_WAL=1`, one log record)
//! as one graph commit, following the shape of the group-commit RFC draft:
//!
//! - A writer prepares as today (capture, execute, validate at its pinned base,
//!   stage its data files) and then submits an entry instead of taking the
//!   branch gate itself.
//! - Entries queue per branch. Whichever submitter holds the branch's leader
//!   lock takes everything queued as one batch (a write group: no spawned
//!   task), so batching is natural and never timed.
//! - Under the schema (shared) and branch gates the leader captures the
//!   current authority once and admits entries in arrival order: same branch
//!   incarnation and schema identity, every commit between the entry's
//!   captured head and the current one published by this group (the effect
//!   log), and ids disjoint from those commits and from the batch.
//! - Each table's admitted effects become one detached commit read from the
//!   table's current pin (`table_store::compose_pure_inserts`), and the batch
//!   publishes through the unchanged publisher with the first entry's lineage
//!   intent. Every admitted entry is acknowledged with that one commit.
//! - A refused entry gets `ReadSetChanged` and prepares again through the
//!   mutation's own bounded loop.
//!
//! Only insertion-only effects with exact ids join a batch; every other write
//! takes the ordinary path, which the branch gate serializes with batches.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::oneshot;

use crate::db::omnigraph::table_ops::Publication;
use crate::db::omnigraph::{Omnigraph, WriteTxn};
use crate::db::{DatasetUpdate, GraphCommit};
use crate::error::{OmniError, Result};
use crate::exec::staging::StagedMutation;

/// Most entries one batch admits.
const BATCH_MAX_ENTRIES: usize = 512;
/// Most published batches the effect log keeps per branch.
const EFFECT_LOG_BATCHES: usize = 256;

/// Research instrument: per-phase time of batches, summed in microseconds.
pub mod stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    pub static BATCHES: AtomicU64 = AtomicU64::new(0);
    pub static ENTRIES: AtomicU64 = AtomicU64::new(0);
    pub static GATES_US: AtomicU64 = AtomicU64::new(0);
    pub static CAPTURE_US: AtomicU64 = AtomicU64::new(0);
    pub static TABLES_US: AtomicU64 = AtomicU64::new(0);
    pub static PUBLISH_US: AtomicU64 = AtomicU64::new(0);
    pub static QUEUE_US: AtomicU64 = AtomicU64::new(0);

    pub fn add(counter: &AtomicU64, since: std::time::Instant) {
        counter.fetch_add(since.elapsed().as_micros() as u64, Ordering::Relaxed);
    }

    /// Take every counter, resetting it.
    pub fn take() -> [u64; 7] {
        [
            &BATCHES, &ENTRIES, &GATES_US, &CAPTURE_US, &TABLES_US, &PUBLISH_US, &QUEUE_US,
        ]
        .map(|counter| counter.swap(0, Ordering::Relaxed))
    }
}

/// Whether group commit is on for this process.
pub(crate) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("OMNIGRAPH_GROUP_COMMIT").is_ok_and(|value| value == "1")
            || crate::db::omnigraph::wal_enabled()
    })
}

/// Whether the leader stages one write per table for a whole batch
/// (`OMNIGRAPH_GROUP_STAGE=leader`) instead of composing the files each
/// writer staged.
pub(crate) fn leader_stages() -> bool {
    static LEADER: OnceLock<bool> = OnceLock::new();
    *LEADER.get_or_init(|| std::env::var("OMNIGRAPH_GROUP_STAGE").is_ok_and(|value| value == "leader"))
}

/// Whether batches pipeline (`OMNIGRAPH_GROUP_PIPELINE=1`, with leader
/// staging and the record log): a batch stages its rows at the published
/// head, then commits each table detached on the previous batch's tip, then
/// appends its record, each stage under its own lock taken in batch order,
/// so batch `j + 1` stages while batch `j` commits and logs.
pub(crate) fn pipelined() -> bool {
    static PIPELINE: OnceLock<bool> = OnceLock::new();
    // Without the record log the log stage is an ordinary `__manifest`
    // publication: the ablation that isolates the record's contribution.
    *PIPELINE.get_or_init(|| {
        std::env::var("OMNIGRAPH_GROUP_PIPELINE").is_ok_and(|value| value == "1")
            && leader_stages()
            && enabled()
    })
}

/// What an entry brings: the effects its writer staged, or the rows its
/// writer validated for the leader to stage with the rest of the batch.
pub(crate) enum Work {
    Staged(StagedMutation),
    Pending(crate::exec::staging::MutationStaging),
}

/// One prepared write waiting for its batch.
pub(crate) struct Entry {
    pub(crate) work: Work,
    pub(crate) txn: WriteTxn,
    pub(crate) lineage_intent: crate::db::manifest::LineageIntent,
    pub(crate) actor: Option<String>,
}

struct Queued {
    entry: Entry,
    reply: oneshot::Sender<Result<GraphCommit>>,
    queued_at: std::time::Instant,
}

/// What one published batch inserted, keyed by the head it produced.
struct EffectRecord {
    parent: Option<String>,
    ids: HashMap<String, Arc<HashSet<String>>>,
}

#[derive(Default)]
struct EffectLog {
    by_head: HashMap<String, EffectRecord>,
    order: VecDeque<String>,
}

impl EffectLog {
    fn clear(&mut self) {
        self.by_head.clear();
        self.order.clear();
    }

    fn record(&mut self, head: String, record: EffectRecord) {
        self.by_head.insert(head.clone(), record);
        self.order.push_back(head);
        while self.order.len() > EFFECT_LOG_BATCHES {
            if let Some(old) = self.order.pop_front() {
                self.by_head.remove(&old);
            }
        }
    }

    /// The ids every commit in `(captured, current]` inserted, per table, when
    /// this group published each of them; `None` when the horizon leaves the
    /// log.
    fn ids_since(
        &self,
        captured: Option<&str>,
        current: Option<&str>,
    ) -> Option<HashMap<String, Vec<Arc<HashSet<String>>>>> {
        let mut since: HashMap<String, Vec<Arc<HashSet<String>>>> = HashMap::new();
        let mut cursor = current.map(str::to_string);
        for _ in 0..=EFFECT_LOG_BATCHES {
            if cursor.as_deref() == captured {
                return Some(since);
            }
            let head = cursor?;
            let record = self.by_head.get(&head)?;
            for (table, ids) in &record.ids {
                since.entry(table.clone()).or_default().push(Arc::clone(ids));
            }
            cursor = record.parent.clone();
        }
        None
    }
}

struct BranchGroup {
    leader: tokio::sync::Mutex<EffectLog>,
    queue: Mutex<VecDeque<Queued>>,
    /// Pipeline mode: the effect log (the leader lock is then only the
    /// staging stage), the ids of batches admitted and not yet published, the
    /// per-table tips the commit stage chains on, and the log stage's lock.
    effects: Mutex<EffectLog>,
    in_flight: Mutex<Vec<(u64, Arc<HashMap<String, HashSet<String>>>)>>,
    next_batch: std::sync::atomic::AtomicU64,
    chain: tokio::sync::Mutex<ChainState>,

    /// Pipeline order: tickets are taken at admission; the commit and log
    /// stages run batches in ticket order; at most `PIPELINE_DEPTH` batches
    /// are in flight, so a full pipeline lets the next batch grow.
    tickets: std::sync::atomic::AtomicU64,
    chain_turn: TurnGate,
    log_turn: TurnGate,
    depth: tokio::sync::Semaphore,
}

impl BranchGroup {
    fn new() -> Self {
        Self {
            leader: tokio::sync::Mutex::default(),
            queue: Mutex::default(),
            effects: Mutex::default(),
            in_flight: Mutex::default(),
            next_batch: std::sync::atomic::AtomicU64::new(0),
            chain: tokio::sync::Mutex::default(),

            tickets: std::sync::atomic::AtomicU64::new(0),
            chain_turn: TurnGate::default(),
            log_turn: TurnGate::default(),
            depth: tokio::sync::Semaphore::new(pipeline_depth()),
        }
    }
}

/// Whether the pipeline prefetches each new tip's index metadata
/// (`OMNIGRAPH_GROUP_PREFETCH=1`).
fn prefetch_indices() -> bool {
    static PREFETCH: OnceLock<bool> = OnceLock::new();
    *PREFETCH.get_or_init(|| std::env::var("OMNIGRAPH_GROUP_PREFETCH").is_ok_and(|value| value == "1"))
}

/// Batches in flight in a pipelined branch group (`OMNIGRAPH_GROUP_DEPTH`).
fn pipeline_depth() -> usize {
    static DEPTH: OnceLock<usize> = OnceLock::new();
    *DEPTH.get_or_init(|| {
        std::env::var("OMNIGRAPH_GROUP_DEPTH")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8)
    })
}

/// A stage's turn: the ticket whose batch may run the stage next.
struct TurnGate(tokio::sync::watch::Sender<u64>);

impl Default for TurnGate {
    fn default() -> Self {
        Self(tokio::sync::watch::Sender::new(0))
    }
}

impl TurnGate {
    async fn wait(&self, ticket: u64) {
        let mut turn = self.0.subscribe();
        let _ = turn.wait_for(|current| *current == ticket).await;
    }

    fn pass(&self, ticket: u64) {
        self.0.send_modify(|current| *current = ticket + 1);
    }
}

/// The pipeline's per-table chain: each table's newest detached version and
/// its logical version, built on by the next batch's commit stage before the
/// batch that wrote it has logged; a failed log stage starts a new generation
/// and every batch built on the old one is refused.
#[derive(Default)]
struct ChainState {
    generation: u64,
    tips: HashMap<String, ChainTip>,
}

struct ChainTip {
    snapshot: crate::storage_layer::SnapshotHandle,
    logical_version: u64,
    version_metadata: crate::db::manifest::TableVersionMetadata,
}

/// The per-handle group committer: one write group per branch.
#[derive(Default)]
pub(crate) struct GroupCommitter {
    branches: Mutex<HashMap<Option<String>, Arc<BranchGroup>>>,
}

impl GroupCommitter {
    /// Whether a writer on `branch` stages its own effects: with
    /// `OMNIGRAPH_GROUP_ADAPTIVE=<n>`, while fewer than `n` entries wait and
    /// batches are in flight (an idle group, where a leader-staged batch would
    /// put the probe and the data put on the critical path); otherwise the
    /// leader stages the batch.
    pub(crate) fn writer_stages(&self, branch: Option<&str>) -> bool {
        static THRESHOLD: OnceLock<Option<usize>> = OnceLock::new();
        let Some(threshold) = *THRESHOLD.get_or_init(|| {
            std::env::var("OMNIGRAPH_GROUP_ADAPTIVE")
                .ok()
                .and_then(|value| value.parse().ok())
        }) else {
            return false;
        };
        let group = self.group(branch);
        let waiting = group.queue.lock().expect("group commit queue poisoned").len();
        let flying = group
            .in_flight
            .lock()
            .expect("group commit in-flight poisoned")
            .len();
        waiting + flying < threshold
    }

    fn group(&self, branch: Option<&str>) -> Arc<BranchGroup> {
        let mut branches = self.branches.lock().expect("group commit registry poisoned");
        Arc::clone(
            branches
                .entry(branch.map(str::to_string))
                .or_insert_with(|| Arc::new(BranchGroup::new())),
        )
    }

    /// Submit a prepared entry and wait for the commit of the batch that
    /// carries it.
    pub(crate) async fn submit(
        &self,
        db: &Omnigraph,
        branch: Option<&str>,
        entry: Entry,
    ) -> Result<GraphCommit> {
        let group = self.group(branch);
        let (reply, mut outcome) = oneshot::channel();
        group
            .queue
            .lock()
            .expect("group commit queue poisoned")
            .push_back(Queued {
                entry,
                reply,
                queued_at: std::time::Instant::now(),
            });
        loop {
            tokio::select! {
                biased;
                result = &mut outcome => {
                    return result.unwrap_or_else(|_| {
                        Err(OmniError::manifest_internal("group commit leader dropped an entry"))
                    });
                }
                mut log = group.leader.lock() => {
                    if let Ok(result) = outcome.try_recv() {
                        return result;
                    }
                    // A full pipeline: wait for room holding the leader lock,
                    // so entries keep queueing into the next batch.
                    let permit = if pipelined() {
                        Some(group.depth.acquire().await.map_err(|_| {
                            OmniError::manifest_internal("group commit pipeline closed")
                        })?)
                    } else {
                        None
                    };
                    if let Ok(result) = outcome.try_recv() {
                        return result;
                    }
                    let batch: Vec<Queued> = {
                        let mut queue = group.queue.lock().expect("group commit queue poisoned");
                        let take = queue.len().min(BATCH_MAX_ENTRIES);
                        queue.drain(..take).collect()
                    };
                    if batch.is_empty() {
                        continue;
                    }
                    {
                        if let Some(permit) = permit {
                            run_pipelined_batch(db, branch, &group, batch, log, permit).await;
                        } else {
                            run_batch(db, branch, batch, &mut log).await;
                        }
                    }
                }
            }
        }
    }
}

fn same_identity(left: &WriteTxn, right: &WriteTxn) -> bool {
    left.authority.branch_identifier == right.authority.branch_identifier
        && left.authority.schema_ir_hash == right.authority.schema_ir_hash
        && left.authority.schema_identity_domain == right.authority.schema_identity_domain
        && left.authority.schema_identity_version == right.authority.schema_identity_version
}

fn refused(reason: &str) -> OmniError {
    OmniError::manifest_read_set_changed(format!("group_commit:{reason}"), None, None)
}

fn fan_out_error(error: &OmniError) -> OmniError {
    if error.is_read_set_changed() {
        return refused("publication");
    }
    // A fenced batch tells every entry it was fenced, typed, so a caller
    // never retries it as an ordinary conflict.
    if let OmniError::Manifest(manifest) = error
        && let Some(crate::error::ManifestConflictDetails::WriterFenced {
            held,
            read,
            current,
        }) = &manifest.details
    {
        return OmniError::manifest_writer_fenced("this branch", *held, *read, *current);
    }
    OmniError::manifest_internal(format!("group commit batch failed: {error}"))
}

async fn run_batch(
    db: &Omnigraph,
    branch: Option<&str>,
    batch: Vec<Queued>,
    log: &mut EffectLog,
) {
    let started = std::time::Instant::now();
    stats::BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    stats::ENTRIES.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
    for queued in &batch {
        stats::QUEUE_US.fetch_add(
            started.duration_since(queued.queued_at).as_micros() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    let gates = match db.write_queue().acquire_branch_write_gates(branch).await {
        Ok(gates) => gates,
        Err(error) => {
            for queued in batch {
                let _ = queued.reply.send(Err(fan_out_error(&error)));
            }
            return;
        }
    };
    stats::add(&stats::GATES_US, started);
    let captured = std::time::Instant::now();
    let base = match db.open_write_txn(branch).await {
        Ok(base) => base,
        Err(error) => {
            for queued in batch {
                let _ = queued.reply.send(Err(fan_out_error(&error)));
            }
            return;
        }
    };
    stats::add(&stats::CAPTURE_US, captured);
    let current = base.authority.graph_head.clone();

    // Admission in arrival order.
    let mut admitted: Vec<Queued> = Vec::new();
    let mut requeue: Vec<Queued> = Vec::new();
    let mut batch_ids: HashMap<String, HashSet<String>> = HashMap::new();
    let mut actor: Option<Option<String>> = None;
    for queued in batch {
        if !same_identity(&queued.entry.txn, &base) {
            let _ = queued.reply.send(Err(refused("identity")));
            continue;
        }
        if actor
            .as_ref()
            .is_some_and(|first| *first != queued.entry.actor)
        {
            requeue.push(queued);
            continue;
        }
        let Some(since) = log.ids_since(
            queued.entry.txn.authority.graph_head.as_deref(),
            current.as_deref(),
        ) else {
            let _ = queued.reply.send(Err(refused("horizon")));
            continue;
        };
        let ids = match &queued.entry.work {
            Work::Staged(staged) => staged.pure_insert_ids_by_table(),
            Work::Pending(staging) => match staging.pending_ids_by_table(base.catalog.system_columns) {
                Ok(ids) => ids,
                Err(error) => {
                    let _ = queued.reply.send(Err(error));
                    continue;
                }
            },
        };
        let conflicts = ids.iter().any(|(table, ids)| {
            since.get(table).is_some_and(|landed| {
                landed
                    .iter()
                    .any(|landed| ids.iter().any(|id| landed.contains(id)))
            }) || batch_ids
                .get(table)
                .is_some_and(|taken| ids.iter().any(|id| taken.contains(id)))
        });
        if conflicts {
            let _ = queued.reply.send(Err(refused("ids")));
            continue;
        }
        for (table, ids) in ids {
            batch_ids
                .entry(table)
                .or_default()
                .extend(ids.iter().cloned());
        }
        actor.get_or_insert_with(|| queued.entry.actor.clone());
        admitted.push(queued);
    }
    if !requeue.is_empty() {
        // Another actor's entries lead a later batch, ahead of new arrivals.
        // The group lock is held, so no other leader can take them first.
        let group = db.write_queue().group_committer().group(branch);
        let group_queue = &group.queue;
        let mut queue = group_queue.lock().expect("group commit queue poisoned");
        for queued in requeue.into_iter().rev() {
            queue.push_front(queued);
        }
    }
    if admitted.is_empty() {
        drop(gates);
        return;
    }

    let outcome = publish_batch(db, branch, &base, &mut admitted, gates).await;
    match outcome {
        Ok(commit) => {
            log.record(
                commit.graph_commit_id.clone(),
                EffectRecord {
                    parent: current,
                    ids: batch_ids
                        .into_iter()
                        .map(|(table, ids)| (table, Arc::new(ids)))
                        .collect(),
                },
            );
            for queued in admitted {
                let _ = queued.reply.send(Ok(commit.clone()));
            }
        }
        Err(error) => {
            if error.is_read_set_changed() {
                log.clear();
            }
            for queued in admitted {
                let _ = queued.reply.send(Err(fan_out_error(&error)));
            }
        }
    }
}

/// Compose, commit detached and publish one admitted batch.
async fn publish_batch(
    db: &Omnigraph,
    branch: Option<&str>,
    base: &WriteTxn,
    admitted: &mut [Queued],
    gates: crate::db::write_queue::BranchWriteGates,
) -> Result<GraphCommit> {
    if admitted
        .iter()
        .all(|queued| matches!(queued.entry.work, Work::Pending(_)))
    {
        return stage_and_publish_batch(db, branch, base, admitted, gates).await;
    }
    let _gates = gates;
    // Group each table's components, in arrival order.
    let mut tables: Vec<(String, Vec<crate::exec::staging::GroupComponent>)> = Vec::new();
    let mut expected: crate::db::manifest::ExpectedTableVersions = HashMap::new();
    for queued in admitted.iter_mut() {
        let Work::Staged(staged) = &mut queued.entry.work else {
            return Err(OmniError::manifest_internal(
                "a batch mixes staged and pending group-commit entries",
            ));
        };
        let (components, entry_expected) = staged.take_group_components()?;
        for component in components {
            match tables
                .iter_mut()
                .find(|(table, _)| *table == component.table_key)
            {
                Some((_, parts)) => parts.push(component),
                None => tables.push((component.table_key.clone(), vec![component])),
            }
        }
        for (identity, expectation) in entry_expected {
            expected.entry(identity).or_insert(expectation);
        }
    }
    // Every expected pin is the table's pin at the batch's head: the effect
    // log proved nothing but this group's insertions landed in between.
    for expectation in expected.values_mut() {
        let entry = base.base.dataset(&expectation.table_key).ok_or_else(|| {
            refused(&format!("table '{}' left the manifest", expectation.table_key))
        })?;
        expectation.table_version = entry.published_dataset_version;
    }

    let table_fork_owner = match base.branch.as_deref() {
        None => None,
        Some(name) => Some(base.base.native_branch().ok_or_else(|| {
            OmniError::manifest_internal(format!(
                "branch '{name}' has no captured native fork owner"
            ))
        })?),
    };
    let witness = base.authority.staging_witness()?;
    let system_columns = base.catalog.system_columns;
    let commits = tables.into_iter().map(|(table_key, parts)| {
        let witness = &witness;
        async move {
            let entry = base
                .base
                .dataset(&table_key)
                .ok_or_else(|| refused(&format!("table '{table_key}' left the manifest")))?
                .clone();
            let path = parts[0].path.clone();
            let snapshot = db.open_pinned_for_write(&path.full_path, &entry).await?;
            let staged = parts
                .into_iter()
                .map(|part| part.staged)
                .collect::<Vec<_>>();
            let composed =
                crate::table_store::compose_pure_inserts(snapshot.dataset(), staged, system_columns)?;
            let declared_full_text =
                crate::db::omnigraph::table_ops::declared_full_text_columns(&base.catalog, &table_key);
            let (detached, identity) = db
                .storage()
                .commit_staged_detached(
                    snapshot,
                    crate::storage_layer::StagedHandle::new(composed),
                    witness,
                    &declared_full_text,
                )
                .await?;
            let state = db.storage().table_state(&path.full_path, &detached).await?;
            let target = entry.published_dataset_version + 1;
            let version_metadata = state
                .version_metadata
                .with_table_fork_owner(table_fork_owner)
                .with_staged(state.version, identity.uuid.clone())
                .with_last_linear_version(entry.version_metadata.last_linear_version());
            db.read_caches()
                .handles
                .insert(
                    &entry.dataset_path,
                    path.table_branch.as_deref(),
                    target,
                    Some(state.version),
                    version_metadata.e_tag(),
                    detached.dataset().clone(),
                )
                .await;
            Ok::<_, OmniError>(DatasetUpdate {
                identity: path.identity,
                type_key: table_key,
                published_dataset_version: target,
                native_dataset_branch: path.table_branch,
                entity_count: state.row_count,
                version_metadata,
            })
        }
    });
    let tables_started = std::time::Instant::now();
    let updates = futures::future::try_join_all(commits).await?;
    stats::add(&stats::TABLES_US, tables_started);
    let publish_started = std::time::Instant::now();
    let first = admitted
        .first_mut()
        .ok_or_else(|| OmniError::manifest_internal("an empty batch reached publication"))?;
    let actor = first.entry.actor.clone();
    let lineage_intent = first.entry.lineage_intent.clone();
    let published = db
        .commit_updates_on_branch_with_expected(
            branch,
            &updates,
            &expected,
            actor.as_deref(),
            base,
            lineage_intent,
            Publication::Publish,
        )
        .await;
    stats::add(&stats::PUBLISH_US, publish_started);
    published
}

/// The leader-staged batch: every entry's validated rows become one staged
/// write per table read from the table's pin at the batch's head (the
/// ordinary `stage_all` keyed writer, so the id probe runs once per batch and
/// the batch adds one fragment per table), committed detached by the ordinary
/// `commit_all` under the gates the leader holds, and published once.
async fn stage_and_publish_batch(
    db: &Omnigraph,
    branch: Option<&str>,
    base: &WriteTxn,
    admitted: &mut [Queued],
    gates: crate::db::write_queue::BranchWriteGates,
) -> Result<GraphCommit> {
    let mut staging: Option<crate::exec::staging::MutationStaging> = None;
    for queued in admitted.iter_mut() {
        let work = std::mem::replace(
            &mut queued.entry.work,
            Work::Pending(crate::exec::staging::MutationStaging::default()),
        );
        let Work::Pending(pending) = work else {
            return Err(OmniError::manifest_internal(
                "a batch mixes staged and pending group-commit entries",
            ));
        };
        let batch = staging.get_or_insert_with(|| {
            crate::exec::staging::MutationStaging::new(pending.write_budget())
        });
        batch.absorb_pending(pending, &base.base)?;
    }
    let staging = staging
        .ok_or_else(|| OmniError::manifest_internal("an empty batch reached publication"))?;
    let tables_started = std::time::Instant::now();
    let staged = staging.stage_all_with_concurrency(db, branch, 8).await?;
    let mut attempt = crate::db::reprepare::WriteAttempt::with_admitted_gates(gates);
    let committed = staged.commit_all(db, branch, base, None, &mut attempt).await?;
    stats::add(&stats::TABLES_US, tables_started);
    let publish_started = std::time::Instant::now();
    let first = admitted
        .first()
        .ok_or_else(|| OmniError::manifest_internal("an empty batch reached publication"))?;
    let published = db
        .commit_updates_on_branch_with_expected(
            branch,
            &committed.updates,
            &committed.expected_versions,
            first.entry.actor.as_deref(),
            committed.carried_txn.as_ref().unwrap_or(base),
            first.entry.lineage_intent.clone(),
            Publication::Publish,
        )
        .await;
    drop(committed.gates);
    stats::add(&stats::PUBLISH_US, publish_started);
    published
}


/// One pipelined batch (see [`pipelined`]). Entered holding the leader lock
/// (`staging_lock`) and a pipeline-depth permit: admission takes a ticket and
/// releases the lock; staging runs beside other batches' stages; the commit
/// stage and the log stage run in ticket order. A batch that fails still takes
/// and passes its turns, so later tickets never wait on it.
async fn run_pipelined_batch(
    db: &Omnigraph,
    branch: Option<&str>,
    group: &Arc<BranchGroup>,
    batch: Vec<Queued>,
    staging_lock: tokio::sync::MutexGuard<'_, EffectLog>,
    _depth: tokio::sync::SemaphorePermit<'_>,
) {
    use std::sync::atomic::Ordering;
    let started = std::time::Instant::now();
    stats::BATCHES.fetch_add(1, Ordering::Relaxed);
    stats::ENTRIES.fetch_add(batch.len() as u64, Ordering::Relaxed);
    for queued in &batch {
        stats::QUEUE_US.fetch_add(
            started.duration_since(queued.queued_at).as_micros() as u64,
            Ordering::Relaxed,
        );
    }
    let fail_all = |batch: Vec<Queued>, error: &OmniError| {
        for queued in batch {
            let _ = queued.reply.send(Err(fan_out_error(error)));
        }
    };
    // Admission, under the leader lock. Every failure here happens before
    // the batch takes a ticket.
    let admitted_batch = async {
        // The schema permit (shared) is held through the log stage: it
        // excludes schema apply, which takes it exclusively. No branch gate:
        // the record's conditional create orders this group's batches with
        // every other publication on the branch.
        let permit = db.write_queue().acquire_schema_shared().await?;
        let base = db.open_write_txn(branch).await?;
        Ok::<_, OmniError>((permit, base))
    }
    .await;
    let (permit, base) = match admitted_batch {
        Ok(admitted) => admitted,
        Err(error) => return fail_all(batch, &error),
    };
    stats::add(&stats::CAPTURE_US, started);
    let current = base.authority.graph_head.clone();
    let batch_id = group.next_batch.fetch_add(1, Ordering::Relaxed);
    let mut admitted: Vec<Queued> = Vec::new();
    let mut batch_ids: HashMap<String, HashSet<String>> = HashMap::new();
    let mut actor: Option<Option<String>> = None;
    let mut requeue: Vec<Queued> = Vec::new();
    {
        let effects = group.effects.lock().expect("group commit effects poisoned");
        let in_flight = group.in_flight.lock().expect("group commit in-flight poisoned");
        for queued in batch {
            if !same_identity(&queued.entry.txn, &base) {
                let _ = queued.reply.send(Err(refused("identity")));
                continue;
            }
            if actor.as_ref().is_some_and(|first| *first != queued.entry.actor) {
                requeue.push(queued);
                continue;
            }
            let Some(since) = effects.ids_since(
                queued.entry.txn.authority.graph_head.as_deref(),
                current.as_deref(),
            ) else {
                let _ = queued.reply.send(Err(refused("horizon")));
                continue;
            };
            let ids = match &queued.entry.work {
                Work::Pending(staging) => {
                    match staging.pending_ids_by_table(base.catalog.system_columns) {
                        Ok(ids) => ids,
                        Err(error) => {
                            let _ = queued.reply.send(Err(error));
                            continue;
                        }
                    }
                }
                Work::Staged(staged) => staged.pure_insert_ids_by_table(),
            };
            let conflicts = ids.iter().any(|(table, ids)| {
                since.get(table).is_some_and(|landed| {
                    landed.iter().any(|landed| ids.iter().any(|id| landed.contains(id)))
                }) || batch_ids
                    .get(table)
                    .is_some_and(|taken| ids.iter().any(|id| taken.contains(id)))
                    || in_flight.iter().any(|(_, flying)| {
                        flying
                            .get(table)
                            .is_some_and(|taken| ids.iter().any(|id| taken.contains(id)))
                    })
            });
            if conflicts {
                let _ = queued.reply.send(Err(refused("ids")));
                continue;
            }
            for (table, ids) in ids {
                batch_ids.entry(table).or_default().extend(ids.iter().cloned());
            }
            actor.get_or_insert_with(|| queued.entry.actor.clone());
            admitted.push(queued);
        }
    }
    if !requeue.is_empty() {
        let mut queue = group.queue.lock().expect("group commit queue poisoned");
        for queued in requeue.into_iter().rev() {
            queue.push_front(queued);
        }
    }
    if admitted.is_empty() {
        return;
    }
    let batch_ids = Arc::new(batch_ids);
    group
        .in_flight
        .lock()
        .expect("group commit in-flight poisoned")
        .push((batch_id, Arc::clone(&batch_ids)));
    let ticket = group.tickets.fetch_add(1, Ordering::SeqCst);

    let mut failure: Option<OmniError> = None;

    // Stage 1: one staged write per table at the published head, beside other
    // batches' stages.
    let staged = async {
        let mut staging: Option<crate::exec::staging::MutationStaging> = None;
        let mut components = Vec::new();
        for queued in admitted.iter_mut() {
            let work = std::mem::replace(
                &mut queued.entry.work,
                Work::Pending(crate::exec::staging::MutationStaging::default()),
            );
            match work {
                // Effects this writer staged itself (an idle group): composed
                // onto the tip in stage 2 with the rest.
                Work::Staged(mut staged) => components.extend(staged.take_group_components()?.0),
                Work::Pending(pending) => staging
                    .get_or_insert_with(|| {
                        crate::exec::staging::MutationStaging::new(pending.write_budget())
                    })
                    .absorb_pending(pending, &base.base)?,
            }
        }
        if let Some(staging) = staging {
            let mut staged = staging.stage_all_with_concurrency(db, branch, 8).await?;
            components.extend(staged.take_group_components()?.0);
        }
        Ok::<_, OmniError>(components)
    }
    .await;
    let components = match staged {
        Ok(components) => components,
        Err(error) => {
            failure = Some(error);
            Vec::new()
        }
    };

    // Stage 2: commit each table on its chain tip, in ticket order. The next
    // batch forms once this one holds the commit stage (hand-over-hand), so
    // it collects everything that arrives while this one stages.
    group.chain_turn.wait(ticket).await;
    drop(staging_lock);
    let tables_started = std::time::Instant::now();
    let mut updates = Vec::new();
    let mut paths = Vec::new();
    let generation;
    {
        let mut chain = group.chain.lock().await;
        generation = chain.generation;
        // A batch whose merged write is not insertion-only (an upsert met an
        // existing key) cannot be recomposed onto the tip: refuse it before any
        // tip moves; its writers prepare again on the ordinary path.
        if failure.is_none()
            && components
                .iter()
                .any(|component| component.staged.pure_insert_ids().is_none())
        {
            failure = Some(refused("not_insertion_only"));
        }
        if failure.is_none() {
            let committed = async {
                let witness = base.authority.staging_witness()?;
                let table_fork_owner =
                    base.branch.as_deref().and_then(|_| base.base.native_branch());
                let mut tables: Vec<(String, crate::exec::staging::StagedTablePath, Vec<crate::table_store::StagedWrite>)> =
                    Vec::new();
                for component in components {
                    match tables.iter_mut().find(|(table, _, _)| *table == component.table_key) {
                        Some((_, _, parts)) => parts.push(component.staged),
                        None => tables.push((component.table_key, component.path, vec![component.staged])),
                    }
                }
                for (table_key, path, parts) in tables {
                    let entry = base.base.dataset(&table_key).cloned().ok_or_else(|| {
                        refused(&format!("table '{table_key}' left the manifest"))
                    })?;
                    let (tip, logical, last_linear) = match chain.tips.get(&table_key) {
                        Some(tip) if tip.logical_version > entry.published_dataset_version => (
                            tip.snapshot.clone(),
                            tip.logical_version,
                            tip.version_metadata.last_linear_version(),
                        ),
                        _ => (
                            db.open_pinned_for_write(&path.full_path, &entry).await?,
                            entry.published_dataset_version,
                            entry.version_metadata.last_linear_version(),
                        ),
                    };
                    let composed = crate::table_store::compose_pure_inserts(
                        tip.dataset(),
                        parts,
                        base.catalog.system_columns,
                    )?;
                    let declared_full_text =
                        crate::db::omnigraph::table_ops::declared_full_text_columns(
                            &base.catalog,
                            &table_key,
                        );
                    let (detached, identity) = db
                        .storage()
                        .commit_staged_detached(
                            tip,
                            crate::storage_layer::StagedHandle::new(composed),
                            &witness,
                            &declared_full_text,
                        )
                        .await?;
                    let state = db
                        .storage()
                        .table_state(&path.full_path, &detached)
                        .await?;
                    let target = logical + 1;
                    let version_metadata = state
                        .version_metadata
                        .with_table_fork_owner(table_fork_owner)
                        .with_staged(state.version, identity.uuid.clone())
                        .with_last_linear_version(last_linear);
                    db.read_caches()
                        .handles
                        .insert(
                            &entry.dataset_path,
                            path.table_branch.as_deref(),
                            target,
                            Some(state.version),
                            version_metadata.e_tag(),
                            detached.dataset().clone(),
                        )
                        .await;
                    // Writers validate against this version next: load its
                    // index metadata into the session cache once, beside the
                    // log stage, instead of once per writer.
                    if prefetch_indices() {
                        let tip = detached.dataset().clone();
                        tokio::spawn(async move {
                            use lance::index::DatasetIndexExt as _;
                            let _ = tip.load_indices().await;
                        });
                    }
                    chain.tips.insert(
                        table_key.clone(),
                        ChainTip {
                            snapshot: detached,
                            logical_version: target,
                            version_metadata: version_metadata.clone(),
                        },
                    );
                    paths.push(path.clone());
                    updates.push(DatasetUpdate {
                        identity: path.identity,
                        type_key: table_key,
                        published_dataset_version: target,
                        native_dataset_branch: path.table_branch,
                        entity_count: state.row_count,
                        version_metadata,
                    });
                }
                Ok::<_, OmniError>(())
            }
            .await;
            if let Err(error) = committed {
                // Tips this batch moved name versions no record will name.
                chain.generation += 1;
                chain.tips.clear();
                failure = Some(error);
            }
        }
    }
    group.chain_turn.pass(ticket);
    stats::add(&stats::TABLES_US, tables_started);

    // Stage 3: the record, in ticket order.
    group.log_turn.wait(ticket).await;
    let publish_started = std::time::Instant::now();
    let published = match failure {
        Some(error) => Err(error),
        None => {
            async {
                if group.chain.lock().await.generation != generation {
                    return Err(refused("pipeline_generation"));
                }
                let now = db.open_write_txn(branch).await?;
                let mut expected: crate::db::manifest::ExpectedTableVersions = HashMap::new();
                for (update, path) in updates.iter().zip(&paths) {
                    let current = now
                        .base
                        .dataset(&update.type_key)
                        .map(|entry| entry.published_dataset_version);
                    if current != Some(update.published_dataset_version - 1) {
                        return Err(refused("pipeline_predecessor"));
                    }
                    expected.insert(
                        update.identity,
                        crate::db::manifest::TableVersionExpectation {
                            table_key: update.type_key.clone(),
                            table_version: update.published_dataset_version - 1,
                            native_ref: path.pinned_native_ref.clone(),
                        },
                    );
                }
                let first = admitted.first().ok_or_else(|| {
                    OmniError::manifest_internal("an empty batch reached its record")
                })?;
                db.commit_updates_on_branch_with_expected(
                    branch,
                    &updates,
                    &expected,
                    first.entry.actor.as_deref(),
                    &now,
                    first.entry.lineage_intent.clone(),
                    Publication::Publish,
                )
                .await
            }
            .await
        }
    };
    match &published {
        Ok(commit) => {
            group.effects.lock().expect("group commit effects poisoned").record(
                commit.graph_commit_id.clone(),
                EffectRecord {
                    parent: commit.parent_commit_id.clone(),
                    ids: batch_ids
                        .iter()
                        .map(|(table, ids)| (table.clone(), Arc::new(ids.clone())))
                        .collect(),
                },
            );
        }
        Err(error) => {
            {
                let mut chain = group.chain.lock().await;
                chain.generation += 1;
                chain.tips.clear();
            }
            if error.is_read_set_changed() {
                group.effects.lock().expect("group commit effects poisoned").clear();
            }
        }
    }
    group.log_turn.pass(ticket);
    stats::add(&stats::PUBLISH_US, publish_started);
    group
        .in_flight
        .lock()
        .expect("group commit in-flight poisoned")
        .retain(|(id, _)| *id != batch_id);
    drop(permit);
    match published {
        Ok(commit) => {
            for queued in admitted {
                let _ = queued.reply.send(Ok(commit.clone()));
            }
        }
        Err(error) => fail_all(admitted, &error),
    }
}
