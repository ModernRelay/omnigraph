//! WAL research prototype: group commit at the branch publisher.
//!
//! Not a production path. Enabled per process by `OMNIGRAPH_GROUP_COMMIT=1`
//! (read once). Several insertion-only mutations on one branch publish with
//! one `__manifest` publication (or, with `OMNIGRAPH_WAL=1`, one log record)
//! as one graph commit, following the shape of the group-commit RFC draft in
//! PR #785:
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

/// One prepared write waiting for its batch.
pub(crate) struct Entry {
    pub(crate) staged: StagedMutation,
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

#[derive(Default)]
struct BranchGroup {
    leader: tokio::sync::Mutex<EffectLog>,
    queue: Mutex<VecDeque<Queued>>,
}

/// The per-handle group committer: one write group per branch.
#[derive(Default)]
pub(crate) struct GroupCommitter {
    branches: Mutex<HashMap<Option<String>, Arc<BranchGroup>>>,
}

impl GroupCommitter {
    fn group(&self, branch: Option<&str>) -> Arc<BranchGroup> {
        let mut branches = self.branches.lock().expect("group commit registry poisoned");
        Arc::clone(
            branches
                .entry(branch.map(str::to_string))
                .or_insert_with(|| Arc::new(BranchGroup::default())),
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
                    let batch: Vec<Queued> = {
                        let mut queue = group.queue.lock().expect("group commit queue poisoned");
                        let take = queue.len().min(BATCH_MAX_ENTRIES);
                        queue.drain(..take).collect()
                    };
                    if !batch.is_empty() {
                        run_batch(db, branch, batch, &mut log).await;
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
        refused("publication")
    } else {
        OmniError::manifest_internal(format!("group commit batch failed: {error}"))
    }
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
        let ids = queued.entry.staged.pure_insert_ids_by_table();
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

    let outcome = publish_batch(db, branch, &base, &mut admitted).await;
    drop(gates);
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
) -> Result<GraphCommit> {
    // Group each table's components, in arrival order.
    let mut tables: Vec<(String, Vec<crate::exec::staging::GroupComponent>)> = Vec::new();
    let mut expected: crate::db::manifest::ExpectedTableVersions = HashMap::new();
    for queued in admitted.iter_mut() {
        let (components, entry_expected) = queued.entry.staged.take_group_components()?;
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
