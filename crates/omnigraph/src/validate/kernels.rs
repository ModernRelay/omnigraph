//! Judgment kernels of the write judge: synchronous functions over rows that
//! are already in memory. The judge fetches committed state and feeds them,
//! from a `load csv` partition or a staged route's batches alike. Every
//! refusal rule (which row is reported, in which order, under which
//! exemption) lives here, so no route judges the same rows differently.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use arrow_array::{Array, RecordBatch};
use datafusion::scalar::ScalarValue;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::EdgeType;

use super::{Violation, string_col};
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

/// Where a partition judges a repeated key: in the [`SharedFirstHolders`]
/// every partition inserts into, so a key repeated anywhere in the input is
/// found as the rows arrive; or inside one batch only, the earliest repeat
/// kept, for a write whose batches are statements in order.
#[derive(Debug)]
pub(super) enum Repeats {
    Shared(Arc<SharedFirstHolders>),
    WithinBatch(Option<(Position, Violation)>),
}

/// Pass 1 of uniqueness over the rows one partition saw: every key is judged
/// for repeats per [`Repeats`], bucketed by shard per batch under the shared
/// holders so nothing merges after the drain; each id's last key by position
/// stays with the partition for the image ([`merge_unique_image`]).
#[derive(Debug)]
pub(super) struct UniquePartial {
    input_only: bool,
    repeats: Repeats,
    final_by_id: FinalKeys,
    buckets: Vec<Vec<Keyed>>,
}

impl UniquePartial {
    /// `input_only` is the replacement's pass: a repeat inside the input is
    /// the only judgment and no image is kept.
    pub(super) fn new(input_only: bool, repeats: Repeats) -> Self {
        let shards = match &repeats {
            Repeats::Shared(holders) => holders.shards.len(),
            Repeats::WithinBatch(_) => 0,
        };
        Self {
            input_only,
            repeats,
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
        let mut seen_in_batch: HashMap<Vec<String>, String> = HashMap::new();
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
            match &mut self.repeats {
                Repeats::Shared(_) => {
                    self.buckets[shard_of(&key, shards)].push((key, id, position));
                }
                Repeats::WithinBatch(first) => {
                    if let Some(prior) = seen_in_batch.insert(key.clone(), id.clone())
                        && first.as_ref().is_none_or(|(at, _)| position < *at)
                    {
                        *first = Some((
                            position,
                            unique_violation(table_key, columns, &key, &id, &prior),
                        ));
                    }
                }
            }
        }
        if let Repeats::Shared(holders) = &self.repeats {
            holders.insert_buckets(table_key, columns, &mut self.buckets);
        }
        Ok(())
    }

    /// The partition's final keys, for the image, and the earliest repeat it
    /// judged inside one batch.
    pub(super) fn into_parts(self) -> (FinalKeys, Option<(Position, Violation)>) {
        let repeat = match self.repeats {
            Repeats::Shared(_) => None,
            Repeats::WithinBatch(first) => first,
        };
        (self.final_by_id, repeat)
    }
}

/// Every partition's final keys merged, the last by position winning and a
/// null key removing its id: the group's image, entries sorted by id.
pub(super) fn merge_unique_image(finals: Vec<FinalKeys>) -> UniqueImage {
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
    UniqueImage { entries }
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
    deleted: &HashSet<&str>,
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
            if !delta_ids.contains(holder) && !deleted.contains(holder.as_str()) {
                sink(unique_violation(table_key, columns, key, id, holder))?;
                break;
            }
        }
    }
    Ok(())
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

/// The srcs a `@card` check counts: the new src of every delta edge, the old
/// src of every removed or changed edge, and every source node the change
/// writes (`written`, filled under a non-zero minimum), minus srcs that are
/// themselves deleted nodes.
pub(super) fn card_affected(
    plan: &CardPlan,
    removed_edges: &[(String, String)],
    moved_from: &[(String, String)],
    written: &HashSet<String>,
    deleted_src_nodes: &HashSet<String>,
) -> HashSet<String> {
    let mut affected: HashSet<String> = plan.delta_by_id.values().cloned().collect();
    for (_, src) in removed_edges.iter().chain(moved_from.iter()) {
        affected.insert(src.clone());
    }
    affected.extend(written.iter().cloned());
    affected.retain(|src| !deleted_src_nodes.contains(src));
    affected
}

/// Count the merged edges per affected src and report each bound it breaks,
/// srcs in sorted order, `max` before `min`. `committed_counts` holds each
/// src's committed edges the delta neither deletes nor re-places; the
/// coalesced delta edges then count at their last-wins src.
pub(super) fn card_violations<F>(
    edge_table: &str,
    edge_type: &EdgeType,
    plan: &CardPlan,
    affected: &HashSet<String>,
    committed_counts: &HashMap<String, u32>,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let card = &edge_type.cardinality;
    let mut delta_counts: HashMap<&str, u32> = HashMap::new();
    for src in plan.delta_by_id.values() {
        *delta_counts.entry(src.as_str()).or_insert(0) += 1;
    }

    let mut srcs: Vec<&String> = affected.iter().collect();
    srcs.sort();
    for src in srcs {
        let count = committed_counts
            .get(src.as_str())
            .copied()
            .unwrap_or(0)
            .saturating_add(delta_counts.get(src.as_str()).copied().unwrap_or(0));
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

    fn collect(violations: &mut Vec<Violation>) -> impl FnMut(Violation) -> Result<()> + '_ {
        move |violation| {
            violations.push(violation);
            Ok(())
        }
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

        let mut partial = UniquePartial::new(true, Repeats::Shared(Arc::clone(&holders)));
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
        let deleted: HashSet<&str> = ["gone"].into_iter().collect();
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
    fn card_affected_counts_delta_removed_and_written_sources_minus_deleted() {
        let plan = CardPlan {
            delta_by_id: [("e1", "s2"), ("e2", "s3")]
                .into_iter()
                .map(|(id, src)| (id.to_string(), src.to_string()))
                .collect(),
            removed_ids: vec!["e9".to_string()],
            changed_ids: vec!["e1".to_string(), "e2".to_string()],
        };

        let removed_edges = vec![("e9".to_string(), "s9".to_string())];
        let deleted_src: HashSet<String> = ["s3".to_string()].into_iter().collect();
        let affected = card_affected(&plan, &removed_edges, &[], &HashSet::new(), &deleted_src);
        assert_eq!(
            affected,
            ["s2".to_string(), "s9".to_string()].into_iter().collect()
        );

        // A written source node with no delta edge is counted; a written
        // source that is also a deleted node is not.
        let written: HashSet<String> = ["s4".to_string(), "s3".to_string()].into_iter().collect();
        let affected = card_affected(&plan, &removed_edges, &[], &written, &deleted_src);
        assert_eq!(
            affected,
            ["s2".to_string(), "s4".to_string(), "s9".to_string()]
                .into_iter()
                .collect()
        );
    }
}
