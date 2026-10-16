//! Judgment kernels of the integrity evaluator: synchronous functions over
//! rows that are already in memory.
//!
//! The evaluator in the parent module fetches committed state and feeds these
//! kernels; a load operator can feed them from its own scan. Every refusal
//! rule (which row is reported, in which order, under which exemption) lives
//! here, so the two routes cannot judge the same rows differently.

use std::collections::{HashMap, HashSet};

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
    within_batch_violation: bool,
}

impl UniqueImageBuilder {
    pub(super) fn for_input() -> Self {
        Self {
            input_keys: Some(HashMap::new()),
            ..Self::default()
        }
    }

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
