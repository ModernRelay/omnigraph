//! Judgment kernels of the integrity evaluator: synchronous functions over
//! rows that are already in memory.
//!
//! The evaluator in the parent module fetches committed state and feeds these
//! kernels; a load operator can feed them from its own scan. Every refusal
//! rule (which row is reported, in which order, under which exemption) lives
//! here, so the two routes cannot judge the same rows differently.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::{Array, RecordBatch};
use datafusion::scalar::ScalarValue;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::EdgeType;

use super::{ChangeSet, TableChange, Violation, string_col};
use crate::error::{MergeConflictKind, OmniError, Result};
use crate::loader::{composite_unique_key, format_tuple};

/// One entry of a unique group's coalesced image: `(id, (canonical key
/// strings, typed key values))`.
pub(super) type FinalKeyByIdEntry = (String, (Vec<String>, Vec<ScalarValue>));

/// Committed holders per canonical key, each holder list in scan order.
pub(super) type HoldersByKey = HashMap<Vec<String>, Vec<String>>;

/// A unique group's final image over the delta: the id-sorted entries holding
/// a non-null key after last-wins coalescing by id.
#[derive(Debug, Default)]
pub(super) struct UniqueImage {
    pub(super) entries: Vec<FinalKeyByIdEntry>,
    /// A key repeated inside one batch was reported to the sink.
    pub(super) within_batch_violation: bool,
}

/// The group's columns of `batch`, in declaration order.
fn group_columns(
    batch: &RecordBatch,
    table_key: &str,
    columns: &[String],
) -> Result<Vec<arrow_array::ArrayRef>> {
    columns
        .iter()
        .map(|name| {
            batch.column_by_name(name).cloned().ok_or_else(|| {
                OmniError::manifest(format!(
                    "graph type {table_key} is missing unique property '{name}'"
                ))
            })
        })
        .collect()
}

/// Pass 1 of uniqueness, one batch at a time: report a key repeated inside
/// ONE batch (two input records sharing a key, even under the same id, since
/// a load has no row order) and coalesce the delta by id, last-wins, into
/// each id's final key. A row whose key became null removes its id from the
/// image. The typed values come from the row's Arrow columns so the
/// committed lookup compares like-typed; a stringified key would push Utf8
/// against a Date, Bool or Float column.
#[derive(Debug, Default)]
pub(super) struct UniqueImageBuilder {
    final_by_id: HashMap<String, (Vec<String>, Vec<ScalarValue>)>,
    input_keys: Option<HashMap<Vec<String>, String>>,
    input_only: bool,
    within_batch_violation: bool,
}

impl UniqueImageBuilder {
    pub(super) fn observe<F>(
        &mut self,
        table_key: &str,
        columns: &[String],
        batch: &RecordBatch,
        system_columns: SystemColumns,
        sink: &mut F,
    ) -> Result<()>
    where
        F: FnMut(Violation) -> Result<()>,
    {
        let group_columns = group_columns(batch, table_key, columns)?;
        let ids = string_col(batch, system_columns.id)?;
        let mut seen_in_batch: HashMap<Vec<String>, String> = HashMap::new();
        let seen = self.input_keys.as_mut().unwrap_or(&mut seen_in_batch);
        for row in 0..batch.num_rows() {
            let id = ids.value(row).to_string();
            let Some(key) = composite_unique_key(&group_columns, row)? else {
                self.final_by_id.remove(&id);
                continue;
            };
            if self.input_only {
                if let Some(prior) = seen.get(&key) {
                    sink(unique_violation(table_key, columns, &key, &id, prior))?;
                    self.within_batch_violation = true;
                }
                seen.insert(key, id);
                continue;
            }
            if let Some(prior) = seen.insert(key.clone(), id.clone()) {
                sink(unique_violation(table_key, columns, &key, &id, &prior))?;
                self.within_batch_violation = true;
            }
            let values = group_columns
                .iter()
                .map(|arr| ScalarValue::try_from_array(arr, row))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| OmniError::manifest(e.to_string()))?;
            self.final_by_id.insert(id, (key, values));
        }
        Ok(())
    }

    pub(super) fn finish(self) -> UniqueImage {
        let mut entries: Vec<FinalKeyByIdEntry> = self.final_by_id.into_iter().collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        UniqueImage {
            entries,
            within_batch_violation: self.within_batch_violation,
        }
    }
}

/// A row's place in the delta, `partition << 48 | slice << 24 | row in
/// slice` (`load::position`), so facts the partitions extracted from
/// different slices compare in source order.
pub(crate) type Position = u64;

/// The first holder of each key by position, `(id, position)`.
pub(super) type FirstHolders = HashMap<Vec<String>, (String, Position)>;

/// Each id's last key by position; `None` is a null key, which removes the
/// id from the image.
pub(super) type FinalKeys = HashMap<String, (Position, Option<(Vec<String>, Vec<ScalarValue>)>)>;

fn shard_of(key: &[String], shards: usize) -> usize {
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    usize::try_from(hasher.finish() % shards as u64).unwrap_or(0)
}

/// One row's key on its way into a shard: the key, its holder's id, its position.
type Keyed = (Vec<String>, String, Position);

/// One shard of the shared holders: the map and the earliest repeat judged
/// into it, under one lock.
#[derive(Debug, Default)]
struct SharedShard {
    holders: FirstHolders,
    first: Option<(Position, Violation)>,
}

/// The first holder of each key over every partition's rows, sharded by the
/// key's hash under one lock per shard, which a partition takes once per
/// batch. A key held by two partitions is judged as the second arrives, at
/// the later holder and named against the earlier, as one pass in source
/// order would report it: the key's two earliest positions always meet,
/// whatever the arrival order, so the earliest repeat is the same. The
/// shards are sized once from the source's row estimate ([`Self::size`]) so
/// no table grows under its lock.
pub(super) struct SharedFirstHolders {
    shards: Vec<Mutex<SharedShard>>,
    sizing: Mutex<Sizing>,
    inserted: AtomicUsize,
}

/// Whether the shards were sized, and the table charge held meanwhile.
enum Sizing {
    Unsized,
    /// `charge` is the sized tables' admitted memory, released once
    /// `covered_after` rows are inserted and the drain's per-row charges
    /// carry the tables; `None` when the pool declined and the shards grow.
    Sized {
        covered_after: usize,
        charge: Option<Box<dyn Send>>,
    },
}

impl std::fmt::Debug for SharedFirstHolders {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedFirstHolders")
            .field("shards", &self.shards.len())
            .field("inserted", &self.inserted.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl SharedFirstHolders {
    pub(super) fn new(shards: usize) -> Self {
        Self {
            shards: (0..shards.max(1))
                .map(|_| Mutex::new(SharedShard::default()))
                .collect(),
            sizing: Mutex::new(Sizing::Unsized),
            inserted: AtomicUsize::new(0),
        }
    }

    /// The bytes the shards' tables take holding `rows` entries, as the
    /// standard map allocates them: buckets to the next power of two over
    /// 8/7 of a shard's share, one control byte each.
    pub(super) fn table_bytes(&self, rows: usize) -> usize {
        let per_shard = rows.div_ceil(self.shards.len());
        let buckets = if per_shard < 8 {
            8
        } else {
            per_shard
                .saturating_mul(8)
                .checked_div(7)
                .unwrap_or(usize::MAX)
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX)
        };
        buckets
            .saturating_mul(std::mem::size_of::<(Vec<String>, (String, Position))>() + 1)
            .saturating_mul(self.shards.len())
    }

    /// Every shard reserved for its share of `rows`, by the first caller only;
    /// `admit` charges [`Self::table_bytes`] and returns the charge, or `None`
    /// to leave the shards growing as they fill. A charge is released once the
    /// inserted rows, at `per_row_charged` bytes each, cover the tables.
    pub(super) fn size(
        &self,
        rows: usize,
        per_row_charged: usize,
        admit: impl FnOnce(usize) -> Option<Box<dyn Send>>,
    ) {
        let mut sizing = self.sizing.lock().unwrap_or_else(PoisonError::into_inner);
        if !matches!(*sizing, Sizing::Unsized) {
            return;
        }
        let bytes = self.table_bytes(rows);
        let charge = admit(bytes);
        if charge.is_some() {
            let per_shard = rows.div_ceil(self.shards.len());
            for shard in &self.shards {
                let mut shard = shard.lock().unwrap_or_else(PoisonError::into_inner);
                let additional = per_shard.saturating_sub(shard.holders.len());
                shard.holders.reserve(additional);
            }
        }
        *sizing = Sizing::Sized {
            covered_after: bytes.div_ceil(per_row_charged.max(1)),
            charge,
        };
    }

    /// The table charge dropped once `inserted` rows carry it.
    fn release_covered_charge(&self, inserted: usize) {
        let mut sizing = self.sizing.lock().unwrap_or_else(PoisonError::into_inner);
        if let Sizing::Sized {
            covered_after,
            charge,
        } = &mut *sizing
            && charge.is_some()
            && inserted >= *covered_after
        {
            *charge = None;
        }
    }

    #[cfg(test)]
    pub(super) fn capacity(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .holders
                    .capacity()
            })
            .sum()
    }

    /// Every bucket's rows into its shard; the buckets are left empty.
    fn insert_buckets(&self, table_key: &str, columns: &[String], buckets: &mut [Vec<Keyed>]) {
        let rows: usize = buckets.iter().map(Vec::len).sum();
        let inserted = self
            .inserted
            .fetch_add(rows, Ordering::Relaxed)
            .saturating_add(rows);
        for (shard, bucket) in self.shards.iter().zip(buckets) {
            if bucket.is_empty() {
                continue;
            }
            let mut shard = shard.lock().unwrap_or_else(PoisonError::into_inner);
            let SharedShard { holders, first } = &mut *shard;
            for (key, id, position) in bucket.drain(..) {
                match holders.entry(key) {
                    Entry::Occupied(mut held) => {
                        let (held_id, held_position) = held.get();
                        let held_position = *held_position;
                        let (early, late, late_position) = if held_position <= position {
                            (held_id.clone(), id, position)
                        } else {
                            (id, held_id.clone(), held_position)
                        };
                        if first.as_ref().is_none_or(|(at, _)| late_position < *at) {
                            *first = Some((
                                late_position,
                                unique_violation(table_key, columns, held.key(), &late, &early),
                            ));
                        }
                        if position < held_position {
                            held.insert((early, position));
                        }
                    }
                    Entry::Vacant(slot) => {
                        slot.insert((id, position));
                    }
                }
            }
        }
        self.release_covered_charge(inserted);
    }

    /// The earliest repeat over every shard.
    pub(super) fn first_repeat(&self) -> Option<(Position, Violation)> {
        self.shards
            .iter()
            .filter_map(|shard| {
                shard
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .first
                    .clone()
            })
            .min_by_key(|(position, _)| *position)
    }

    /// The shards' maps taken out, one box each, so the caller frees them on
    /// workers of its choosing; the shards are left empty and the table
    /// charge, if still held, released.
    pub(super) fn take_maps(&self) -> Vec<Box<dyn Send>> {
        *self.sizing.lock().unwrap_or_else(PoisonError::into_inner) = Sizing::Sized {
            covered_after: 0,
            charge: None,
        };
        self.shards
            .iter()
            .map(|shard| {
                let map = std::mem::take(
                    &mut shard.lock().unwrap_or_else(PoisonError::into_inner).holders,
                );
                Box::new(map) as Box<dyn Send>
            })
            .collect()
    }
}

/// Pass 1 of uniqueness over the rows one partition saw: every key goes into
/// the [`SharedFirstHolders`] all partitions insert into, bucketed by shard
/// per batch, so repeats across partitions are judged as the rows arrive and
/// nothing merges after the drain; each id's last key by position stays with
/// the partition for the image ([`merge_unique_image`]).
#[derive(Debug)]
pub(super) struct UniquePartial {
    input_only: bool,
    holders: Arc<SharedFirstHolders>,
    final_by_id: FinalKeys,
    buckets: Vec<Vec<Keyed>>,
}

impl UniquePartial {
    /// `input_only` is the replacement's pass: a repeat inside the input is
    /// the only judgment and no image is kept.
    pub(super) fn new(input_only: bool, holders: Arc<SharedFirstHolders>) -> Self {
        let shards = holders.shards.len();
        Self {
            input_only,
            holders,
            final_by_id: HashMap::new(),
            buckets: (0..shards).map(|_| Vec::new()).collect(),
        }
    }

    /// `base` is the position of the batch's first row.
    pub(super) fn observe(
        &mut self,
        table_key: &str,
        columns: &[String],
        batch: &RecordBatch,
        system_columns: SystemColumns,
        base: Position,
    ) -> Result<()> {
        let group_columns = group_columns(batch, table_key, columns)?;
        let ids = string_col(batch, system_columns.id)?;
        let shards = self.buckets.len();
        for row in 0..batch.num_rows() {
            let position = base.saturating_add(row as Position);
            let id = ids.value(row).to_string();
            let Some(key) = composite_unique_key(&group_columns, row)? else {
                if !self.input_only {
                    self.final_by_id.insert(id, (position, None));
                }
                continue;
            };
            if !self.input_only {
                let values = group_columns
                    .iter()
                    .map(|arr| ScalarValue::try_from_array(arr, row))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| OmniError::manifest(e.to_string()))?;
                self.final_by_id
                    .insert(id.clone(), (position, Some((key.clone(), values))));
            }
            self.buckets[shard_of(&key, shards)].push((key, id, position));
        }
        self.holders
            .insert_buckets(table_key, columns, &mut self.buckets);
        Ok(())
    }

    /// The partition's final keys, for the image.
    pub(super) fn into_final_keys(self) -> FinalKeys {
        self.final_by_id
    }
}

/// Every partition's final keys merged, the last by position winning and a
/// null key removing its id: the group's image, entries sorted by id.
pub(super) fn merge_unique_image(finals: Vec<FinalKeys>, repeated: bool) -> UniqueImage {
    let mut merged: FinalKeys = HashMap::new();
    for map in finals {
        for (id, (position, key)) in map {
            match merged.entry(id) {
                Entry::Occupied(mut held) => {
                    if position > held.get().0 {
                        held.insert((position, key));
                    }
                }
                Entry::Vacant(slot) => {
                    slot.insert((position, key));
                }
            }
        }
    }
    let mut entries: Vec<FinalKeyByIdEntry> = merged
        .into_iter()
        .filter_map(|(id, (_, key))| key.map(|key| (id, key)))
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    UniqueImage {
        entries,
        within_batch_violation: repeated,
    }
}

/// [`UniqueImageBuilder`] over every value batch of `change`.
pub(super) fn unique_image<F>(
    table_key: &str,
    columns: &[String],
    change: &TableChange,
    system_columns: SystemColumns,
    sink: &mut F,
) -> Result<UniqueImage>
where
    F: FnMut(Violation) -> Result<()>,
{
    let mut builder = UniqueImageBuilder::default();
    for batch in change.value_batches() {
        builder.observe(table_key, columns, batch, system_columns, sink)?;
    }
    Ok(builder.finish())
}

/// Pass 2 of uniqueness: two DISTINCT ids holding the same final key. Reported
/// in id order, each later holder against the one before it.
pub(super) fn unique_coalesced_dups<F>(
    table_key: &str,
    columns: &[String],
    entries: &[FinalKeyByIdEntry],
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let mut holder_by_key: HashMap<&Vec<String>, &String> = HashMap::new();
    for (id, (key, _)) in entries {
        if let Some(other) = holder_by_key.insert(key, id) {
            if other != id {
                sink(unique_violation(table_key, columns, key, id, other))?;
            }
        }
    }
    Ok(())
}

/// The distinct final keys of `entries`, first occurrence order, as the
/// `(canonical, typed)` pairs a committed lookup takes.
pub(super) fn unique_probe_keys(
    entries: &[FinalKeyByIdEntry],
) -> Vec<(Vec<String>, Vec<ScalarValue>)> {
    let mut seen: HashSet<&Vec<String>> = HashSet::new();
    entries
        .iter()
        .filter(|(_, (key, _))| seen.insert(key))
        .map(|(_, (key, values))| (key.clone(), values.clone()))
        .collect()
}

/// Append the committed holders of the `wanted` tuples found in one scanned
/// `batch` to `holders`, in row order. Rows with a null id or a null key
/// column hold no tuple.
pub(super) fn match_unique_holders(
    batch: &RecordBatch,
    table_key: &str,
    columns: &[String],
    system_columns: SystemColumns,
    wanted: &HashSet<&Vec<String>>,
    holders: &mut HoldersByKey,
) -> Result<()> {
    let ids = string_col(batch, system_columns.id)?;
    let group_columns = group_columns(batch, table_key, columns)?;
    for row in 0..batch.num_rows() {
        if ids.is_null(row) {
            continue;
        }
        let Some(key) = composite_unique_key(&group_columns, row)? else {
            continue;
        };
        if wanted.contains(&key) {
            holders
                .entry(key)
                .or_default()
                .push(ids.value(row).to_string());
        }
    }
    Ok(())
}

/// Pass 3 of uniqueness: a final key held by a SURVIVING committed row, one
/// that is neither in the delta (an upsert of itself) nor deleted. Entries
/// are visited in id order; the first surviving holder in scan order is the
/// one reported.
pub(super) fn unique_committed_collisions<F>(
    table_key: &str,
    columns: &[String],
    entries: &[FinalKeyByIdEntry],
    holders_by_key: &HoldersByKey,
    delta_ids: &HashSet<String>,
    deleted: &HashSet<&String>,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    for (id, (key, _)) in entries {
        let Some(holders) = holders_by_key.get(key) else {
            continue;
        };
        for holder in holders {
            if !delta_ids.contains(holder) && !deleted.contains(holder) {
                sink(unique_violation(table_key, columns, key, id, holder))?;
                break;
            }
        }
    }
    Ok(())
}

/// Append `(edge id, src, dst)` per raw row of one delta batch to `edges`.
pub(super) fn edge_ri_batch(
    batch: &RecordBatch,
    system_columns: SystemColumns,
    edges: &mut Vec<(String, String, String)>,
) -> Result<()> {
    let ids = string_col(batch, system_columns.id)?;
    let srcs = string_col(batch, system_columns.src)?;
    let dsts = string_col(batch, system_columns.dst)?;
    for i in 0..batch.num_rows() {
        edges.push((
            ids.value(i).to_string(),
            srcs.value(i).to_string(),
            dsts.value(i).to_string(),
        ));
    }
    Ok(())
}

/// `(edge id, src, dst)` per raw added or changed delta row.
pub(super) fn edge_ri_delta(
    change: &TableChange,
    system_columns: SystemColumns,
) -> Result<Vec<(String, String, String)>> {
    let mut edges = Vec::new();
    for batch in change.value_batches() {
        edge_ri_batch(batch, system_columns, &mut edges)?;
    }
    Ok(edges)
}

/// Split endpoint `ids` by what the node delta settles: an added or changed
/// id exists, a deleted id does not, and every other id goes to the
/// committed lookup (`to_probe`, in input order, repeats kept).
pub(super) fn partition_endpoints(
    ids: &[String],
    added_changed: &HashSet<String>,
    deleted: &HashSet<String>,
) -> (HashSet<String>, Vec<String>) {
    let mut exist = HashSet::new();
    let mut to_probe = Vec::new();
    for id in ids {
        if added_changed.contains(id) {
            exist.insert(id.clone());
        } else if !deleted.contains(id) {
            to_probe.push(id.clone());
        }
    }
    (exist, to_probe)
}

/// Path-a of edge RI: each delta edge whose src or dst is absent from the
/// merged node universe, in delta row order, src before dst.
pub(super) fn orphans_in_delta<F>(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    edges: &[(String, String, String)],
    from_exist: &HashSet<String>,
    to_exist: &HashSet<String>,
    system_columns: SystemColumns,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    for (id, src, dst) in edges {
        if !from_exist.contains(src) {
            sink(orphan_violation(
                edge_table,
                id,
                system_columns.src,
                src,
                from_type,
            ))?;
        }
        if !to_exist.contains(dst) {
            sink(orphan_violation(
                edge_table,
                id,
                system_columns.dst,
                dst,
                to_type,
            ))?;
        }
    }
    Ok(())
}

/// The node ids `changeset` deletes from the edge's `from_table` and `to_table`.
pub(super) fn deleted_endpoints(
    changeset: &ChangeSet,
    from_table: &str,
    to_table: &str,
) -> (Vec<String>, Vec<String>) {
    let deleted_of = |table: &str| {
        changeset
            .get(table)
            .map(|change| change.deleted_ids.clone())
            .unwrap_or_default()
    };
    (deleted_of(from_table), deleted_of(to_table))
}

/// Path-b of edge RI: a committed edge that survives this change (neither in
/// the delta nor removed) while a node it references is deleted. Reachable
/// when the edge lives on the merge target and the node deletion on the
/// source, so no cascade removed it. `retained` is visited in scan order, src
/// before dst.
#[allow(clippy::too_many_arguments)]
pub(super) fn orphans_retained<F>(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    retained: &[(String, String, String)],
    delta_edge_ids: &HashSet<String>,
    removed: &HashSet<&String>,
    deleted_from: &[String],
    deleted_to: &[String],
    system_columns: SystemColumns,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let from_set: HashSet<&String> = deleted_from.iter().collect();
    let to_set: HashSet<&String> = deleted_to.iter().collect();
    for (id, src, dst) in retained {
        if delta_edge_ids.contains(id) || removed.contains(id) {
            continue;
        }
        if from_set.contains(src) {
            sink(orphan_violation(
                edge_table,
                id,
                system_columns.src,
                src,
                from_type,
            ))?;
        }
        if to_set.contains(dst) {
            sink(orphan_violation(
                edge_table,
                id,
                system_columns.dst,
                dst,
                to_type,
            ))?;
        }
    }
    Ok(())
}

/// What a `@card` check needs from the delta: the coalesced edges and the
/// committed ids whose old src must be recounted.
#[derive(Debug, Default)]
pub(super) struct CardPlan {
    /// Delta edges coalesced by id, last-wins, as commit persists them.
    pub(super) delta_by_id: HashMap<String, String>,
    /// Ids the delta deletes.
    pub(super) removed_ids: Vec<String>,
    /// Ids the delta adds or changes; a committed one may move off its src.
    pub(super) changed_ids: Vec<String>,
}

/// Fold one delta batch into `delta_by_id`, last-wins per edge id.
pub(super) fn card_observe(
    batch: &RecordBatch,
    system_columns: SystemColumns,
    delta_by_id: &mut HashMap<String, String>,
) -> Result<()> {
    let ids = string_col(batch, system_columns.id)?;
    let srcs = string_col(batch, system_columns.src)?;
    for i in 0..batch.num_rows() {
        delta_by_id.insert(ids.value(i).to_string(), srcs.value(i).to_string());
    }
    Ok(())
}

/// Coalesce the delta by edge id, last-wins. A Merge load can list the same
/// edge id twice with different srcs; counting raw rows would place one id
/// under several srcs.
pub(super) fn card_plan(change: &TableChange, system_columns: SystemColumns) -> Result<CardPlan> {
    let mut delta_by_id: HashMap<String, String> = HashMap::new();
    for batch in change.value_batches() {
        card_observe(batch, system_columns, &mut delta_by_id)?;
    }
    let changed_ids = delta_by_id.keys().cloned().collect();
    Ok(CardPlan {
        delta_by_id,
        removed_ids: change.deleted_ids.clone(),
        changed_ids,
    })
}

/// The srcs whose edge count the change can move: the new src of every delta
/// edge and the old committed src of every removed or changed edge, minus
/// srcs that are themselves deleted nodes.
pub(super) fn card_affected(
    plan: &CardPlan,
    removed_edges: &[(String, String)],
    moved_from: &[(String, String)],
    deleted_src_nodes: &HashSet<String>,
) -> HashSet<String> {
    let mut affected: HashSet<String> = plan.delta_by_id.values().cloned().collect();
    for (_, src) in removed_edges.iter().chain(moved_from.iter()) {
        affected.insert(src.clone());
    }
    affected.retain(|src| !deleted_src_nodes.contains(src));
    affected
}

/// Count the merged edge-id set per affected src and report each bound it
/// breaks, srcs in sorted order, `max` before `min`. A committed edge leaves
/// its src when the delta deletes or re-places it; the coalesced delta edges
/// then count at their last-wins src.
pub(super) fn card_violations<F>(
    edge_table: &str,
    edge_type: &EdgeType,
    plan: &CardPlan,
    affected: &HashSet<String>,
    committed_for_affected: &[(String, String)],
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let card = &edge_type.cardinality;
    let removed_id_set: HashSet<&String> = plan.removed_ids.iter().collect();
    let mut per_src: HashMap<&String, HashSet<&String>> = HashMap::new();
    for (id, src) in committed_for_affected {
        if removed_id_set.contains(id) || plan.delta_by_id.contains_key(id) {
            continue;
        }
        per_src.entry(src).or_default().insert(id);
    }
    for (id, src) in &plan.delta_by_id {
        per_src.entry(src).or_default().insert(id);
    }

    let mut srcs: Vec<&String> = affected.iter().collect();
    srcs.sort();
    for src in srcs {
        let count = per_src.get(src).map(|ids| ids.len() as u32).unwrap_or(0);
        if let Some(max) = card.max {
            if count > max {
                sink(cardinality_violation(
                    edge_table,
                    &edge_type.name,
                    src,
                    count,
                    "max",
                    max,
                ))?;
            }
        }
        if count < card.min {
            sink(cardinality_violation(
                edge_table,
                &edge_type.name,
                src,
                count,
                "min",
                card.min,
            ))?;
        }
    }
    Ok(())
}

/// Canonical `@unique` violation message, matching the write path's format
/// (`validators.rs` asserts the `"@unique violation on {Type}.{cols}"` prefix via
/// `.contains`). `type_name` is the bare type (`User`), not the `node:`/`edge:`
/// table key; `columns`/`key` render via `format_tuple` (single → `email`,
/// composite → `(a, b)`).
pub(super) fn unique_violation(
    table_key: &str,
    columns: &[String],
    key: &[String],
    id: &str,
    other: &str,
) -> Violation {
    let type_name = table_key
        .strip_prefix("node:")
        .or_else(|| table_key.strip_prefix("edge:"))
        .unwrap_or(table_key);
    Violation {
        table_key: table_key.to_string(),
        row_id: Some(id.to_string()),
        kind: MergeConflictKind::UniqueViolation,
        message: format!(
            "@unique violation on {type_name}.{}: value '{}' held by '{other}' and '{id}'",
            format_tuple(columns),
            format_tuple(key)
        ),
    }
}

/// Canonical orphan-edge message, matching the write path's `"{src|dst} '{id}'
/// not found in {Type}"` format.
pub(super) fn orphan_violation(
    edge_table: &str,
    edge_id: &str,
    label: &str,
    endpoint: &str,
    node_type: &str,
) -> Violation {
    Violation {
        table_key: edge_table.to_string(),
        row_id: Some(edge_id.to_string()),
        kind: MergeConflictKind::OrphanEdge,
        message: format!("{label} '{endpoint}' not found in {node_type}"),
    }
}

pub(super) fn cardinality_violation(
    edge_table: &str,
    edge_name: &str,
    src: &str,
    count: u32,
    bound: &str,
    limit: u32,
) -> Violation {
    Violation {
        table_key: edge_table.to_string(),
        row_id: None,
        kind: MergeConflictKind::CardinalityViolation,
        message: format!(
            "@card violation on edge {edge_name}: source '{src}' has {count} edges ({bound} {limit})"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::catalog::schema_ir::SYSTEM_COLUMNS_V3;

    const SC: SystemColumns = SYSTEM_COLUMNS_V3;
    const TABLE: &str = "node:Doc";

    fn columns() -> Vec<String> {
        vec!["slug".to_string()]
    }

    /// A `node:Doc` batch of `(id, slug)` rows; a `None` slug is a null key.
    fn slug_batch(rows: &[(&str, Option<&str>)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SC.id, DataType::Utf8, false),
            Field::new("slug", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, slug)| *slug).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    fn change_of(batches: Vec<RecordBatch>) -> TableChange {
        TableChange {
            added: batches,
            changed: Vec::new(),
            deleted_ids: Vec::new(),
        }
    }

    fn collect(violations: &mut Vec<Violation>) -> impl FnMut(Violation) -> Result<()> + '_ {
        move |violation| {
            violations.push(violation);
            Ok(())
        }
    }

    fn image_keys(image: &UniqueImage) -> Vec<(&str, &[String])> {
        image
            .entries
            .iter()
            .map(|(id, (key, _))| (id.as_str(), key.as_slice()))
            .collect()
    }

    #[test]
    fn unique_image_of_empty_delta_is_empty() {
        let mut violations = Vec::new();
        let image = unique_image(
            TABLE,
            &columns(),
            &TableChange::default(),
            SC,
            &mut collect(&mut violations),
        )
        .unwrap();
        assert!(image.entries.is_empty());
        assert!(!image.within_batch_violation);
        assert!(violations.is_empty());
    }

    #[test]
    fn unique_image_drops_an_id_whose_key_goes_null() {
        let mut violations = Vec::new();
        let change = change_of(vec![
            slug_batch(&[("a", Some("k1")), ("b", None)]),
            slug_batch(&[("a", None)]),
        ]);
        let image = unique_image(
            TABLE,
            &columns(),
            &change,
            SC,
            &mut collect(&mut violations),
        )
        .unwrap();
        assert!(image.entries.is_empty(), "{:?}", image_keys(&image));
        assert!(violations.is_empty());
    }

    #[test]
    fn unique_image_keeps_the_last_key_of_a_repeated_id() {
        let mut violations = Vec::new();
        let change = change_of(vec![
            slug_batch(&[("a", Some("k1")), ("b", Some("k2"))]),
            slug_batch(&[("a", Some("k3"))]),
        ]);
        let image = unique_image(
            TABLE,
            &columns(),
            &change,
            SC,
            &mut collect(&mut violations),
        )
        .unwrap();
        assert_eq!(
            image_keys(&image),
            vec![
                ("a", &["k3".to_string()][..]),
                ("b", &["k2".to_string()][..])
            ]
        );
        assert!(violations.is_empty());
    }

    #[test]
    fn unique_image_reports_a_key_repeated_inside_one_batch_even_under_one_id() {
        let mut violations = Vec::new();
        let change = change_of(vec![slug_batch(&[("a", Some("k1")), ("a", Some("k1"))])]);
        let image = unique_image(
            TABLE,
            &columns(),
            &change,
            SC,
            &mut collect(&mut violations),
        )
        .unwrap();
        assert!(image.within_batch_violation);
        assert_eq!(
            violations,
            vec![unique_violation(
                TABLE,
                &columns(),
                &["k1".to_string()],
                "a",
                "a"
            )]
        );
    }

    /// A charge guard whose drop records the release.
    struct Released(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for Released {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    #[test]
    fn shared_holders_are_sized_once_and_release_the_charge_when_rows_cover_it() {
        let holders = Arc::new(SharedFirstHolders::new(2));
        assert_eq!(holders.capacity(), 0);
        let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let bytes = holders.table_bytes(1000);
        let charged = Arc::clone(&released);
        holders.size(1000, bytes, |asked| {
            assert_eq!(asked, bytes);
            Some(Box::new(Released(charged)) as Box<dyn Send>)
        });
        assert!(holders.capacity() >= 1000, "{}", holders.capacity());
        assert!(!released.load(Ordering::Relaxed));
        holders.size(5, 1, |_| panic!("sized twice"));

        let mut partial = UniquePartial::new(true, Arc::clone(&holders));
        partial
            .observe(TABLE, &columns(), &slug_batch(&[("a", Some("k1"))]), SC, 0)
            .unwrap();
        assert!(
            released.load(Ordering::Relaxed),
            "one row at the table's bytes covers the charge"
        );

        let declined = SharedFirstHolders::new(2);
        declined.size(1000, 1, |_| None);
        assert!(declined.capacity() < 1000);
        declined.size(1000, 1, |_| panic!("a declined sizing is not retried"));
    }

    #[test]
    fn coalesced_dups_report_each_later_id_against_the_one_before() {
        let entries: Vec<FinalKeyByIdEntry> = ["a", "b", "c"]
            .iter()
            .map(|id| {
                (
                    id.to_string(),
                    (vec!["k".to_string()], vec![ScalarValue::from("k")]),
                )
            })
            .collect();
        let mut violations = Vec::new();
        unique_coalesced_dups(TABLE, &columns(), &entries, &mut collect(&mut violations)).unwrap();
        assert_eq!(
            violations,
            vec![
                unique_violation(TABLE, &columns(), &["k".to_string()], "b", "a"),
                unique_violation(TABLE, &columns(), &["k".to_string()], "c", "b"),
            ]
        );
        assert_eq!(unique_probe_keys(&entries).len(), 1);
    }

    #[test]
    fn committed_collisions_skip_delta_and_deleted_holders_and_report_the_first_survivor() {
        let key = vec!["k".to_string()];
        let entries: Vec<FinalKeyByIdEntry> =
            vec![("z".to_string(), (key.clone(), vec![ScalarValue::from("k")]))];
        let mut holders = HoldersByKey::new();
        holders.insert(
            key.clone(),
            vec![
                "z".to_string(),
                "gone".to_string(),
                "h1".to_string(),
                "h2".to_string(),
            ],
        );
        let delta_ids: HashSet<String> = ["z".to_string()].into_iter().collect();
        let gone = "gone".to_string();
        let deleted: HashSet<&String> = [&gone].into_iter().collect();
        let mut violations = Vec::new();
        unique_committed_collisions(
            TABLE,
            &columns(),
            &entries,
            &holders,
            &delta_ids,
            &deleted,
            &mut collect(&mut violations),
        )
        .unwrap();
        assert_eq!(
            violations,
            vec![unique_violation(TABLE, &columns(), &key, "z", "h1")]
        );
    }

    #[test]
    fn partition_endpoints_settles_delta_ids_and_probes_the_rest() {
        let added: HashSet<String> = ["new".to_string()].into_iter().collect();
        let deleted: HashSet<String> = ["old".to_string()].into_iter().collect();
        let ids = [
            "new".to_string(),
            "old".to_string(),
            "c".to_string(),
            "c".to_string(),
        ];
        let (exist, to_probe) = partition_endpoints(&ids, &added, &deleted);
        assert_eq!(exist, ["new".to_string()].into_iter().collect());
        assert_eq!(to_probe, vec!["c".to_string(), "c".to_string()]);
        assert_eq!(
            partition_endpoints(&[], &added, &deleted),
            (HashSet::new(), Vec::new())
        );
    }

    #[test]
    fn edge_ri_delta_of_empty_change_is_empty() {
        assert!(
            edge_ri_delta(&TableChange::default(), SC)
                .unwrap()
                .is_empty()
        );
        assert!(
            card_plan(&TableChange::default(), SC)
                .unwrap()
                .delta_by_id
                .is_empty()
        );
    }

    #[test]
    fn card_plan_keeps_the_last_src_of_a_repeated_edge_id() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(SC.id, DataType::Utf8, false),
            Field::new(SC.src, DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["e1", "e1", "e2"])) as _,
                Arc::new(StringArray::from(vec!["s1", "s2", "s3"])) as _,
            ],
        )
        .unwrap();
        let mut change = change_of(vec![batch]);
        change.deleted_ids.push("e9".to_string());
        let plan = card_plan(&change, SC).unwrap();
        assert_eq!(plan.delta_by_id.get("e1").map(String::as_str), Some("s2"));
        assert_eq!(plan.delta_by_id.len(), 2);
        assert_eq!(plan.removed_ids, vec!["e9".to_string()]);

        let removed_edges = vec![("e9".to_string(), "s9".to_string())];
        let deleted_src: HashSet<String> = ["s3".to_string()].into_iter().collect();
        let affected = card_affected(&plan, &removed_edges, &[], &deleted_src);
        assert_eq!(
            affected,
            ["s2".to_string(), "s9".to_string()].into_iter().collect()
        );
    }
}
