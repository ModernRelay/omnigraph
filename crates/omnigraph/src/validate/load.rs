//! The one-pass judgment of a load plan's `Validate` node: every check's
//! facts are extracted from each delta batch once while it is hot, then the
//! checks are judged in list order (the refusal precedence) after the last
//! batch, each consulting the committed image under its physical access. The
//! per-batch builders and the finalizers are the evaluator's kernels, so this
//! route and the mutation evaluator cannot judge the same rows differently.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::{Catalog, EdgeType};
use omnigraph_compiler::ir::{Endpoint, ValidateCheck};
use omnigraph_compiler::types::ScalarType;
use omnigraph_planner::{EndpointProbe, ValidateAccess, ValidateRemoval, ValidateSpec};

use super::kernels::{
    CardPlan, FinalKeys, Position, SharedFirstHolders, UniqueImage, UniquePartial, card_affected,
    card_violations, merge_unique_image, orphan_violation, orphans_retained, unique_coalesced_dups,
    unique_committed_collisions, unique_probe_keys,
};
use super::key_set::{IntKeySet, IntKeySetLayout, canonical_i64, key_bytes};
use super::{
    AccessPath, CommittedState, TableChange, Violation, integer_capacity_error,
    overwrite_removed_ids, scan_int_ids, string_col, value_violation,
};
use crate::db::Snapshot;
use crate::error::{OmniError, Result};
use crate::loader::read_reuse::LoadReadReuse;
use crate::loader::{LoadMode, enum_constraint_failures, value_constraint_failures};

pub(crate) enum LoadPhase {
    RemovedInput,
}

pub(super) type ByteAdmission<'a> = dyn FnMut(usize) -> Result<Box<dyn Send>> + Send + 'a;

type PhaseAdmission<'a> = dyn FnMut(LoadPhase) -> Result<Box<dyn Send>> + Send + 'a;
type ReadReuseAdmission<'a> = dyn FnMut() -> Option<LoadReadReuse> + Send + 'a;

pub(crate) struct LoadAdmission<'a> {
    pub(crate) phase: Option<&'a mut PhaseAdmission<'a>>,
    pub(crate) bytes: Option<&'a mut ByteAdmission<'a>>,
    pub(crate) reuse: Option<&'a mut ReadReuseAdmission<'a>>,
    pub(crate) removed: &'a mut (dyn FnMut(&str) -> Result<()> + Send),
    pub(crate) retained: &'a mut (dyn FnMut(&RecordBatch) -> Result<()> + Send),
}

impl LoadAdmission<'_> {
    fn reserve_bytes(&mut self, bytes: usize) -> Result<Box<dyn Send>> {
        match self.bytes.as_mut() {
            Some(admit) => admit(bytes),
            None => Ok(Box::new(())),
        }
    }
}

/// Judge `batches`, the whole delta of `table`, against `checks` in order.
/// `Ok(())` admits the delta; the first violation in check order, then in
/// the check's own order, is the error, with the evaluator's text.
/// Admission charges removed ids and the first retained edge before copying them.
/// The engine sweeps per partition and calls [`judge_load_swept`]; this
/// sequential form is the tests' door.
#[cfg(test)]
pub(crate) async fn judge_load(
    table: &str,
    checks: &[ValidateSpec],
    catalog: &Catalog,
    base: &Snapshot,
    mode: LoadMode,
    batches: &[RecordBatch],
    admission: &mut LoadAdmission<'_>,
) -> Result<()> {
    let extracted = Extracted::sweep(
        table,
        checks,
        catalog,
        batches,
        catalog.system_columns,
        mode == LoadMode::Overwrite,
    )?;
    judge_load_swept(
        table,
        checks,
        ValidateRemoval::Scan,
        catalog,
        base,
        mode,
        batches,
        batches.iter().map(RecordBatch::num_rows).sum(),
        extracted,
        admission,
    )
    .await
}

/// [`judge_load`] with the sweep already done by the caller: the engine's
/// `ValidateExec` sweeps each partition's batches as they arrive and merges
/// the partials ([`Extracted::prepare`], [`Merging::finish`]). `removal` is
/// the plan's word on the ids an overwrite removes: under `Scan` they are
/// found before the checks run, under `None` the plan carries no check that
/// reads them, which the committed table's row count must bear out.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn judge_load_swept(
    table: &str,
    checks: &[ValidateSpec],
    removal: ValidateRemoval,
    catalog: &Catalog,
    base: &Snapshot,
    mode: LoadMode,
    batches: &[RecordBatch],
    delta_rows: usize,
    mut extracted: Extracted,
    admission: &mut LoadAdmission<'_>,
) -> Result<()> {
    let system_columns = catalog.system_columns;
    let overwrite = mode == LoadMode::Overwrite;
    if overwrite && removal == ValidateRemoval::None {
        removal_omitted_soundly(table, checks, catalog, base).await?;
    }
    let removed = if overwrite && removal == ValidateRemoval::Scan {
        let _phase = admission
            .phase
            .as_mut()
            .map(|admit| admit(LoadPhase::RemovedInput))
            .transpose()?;
        let change = TableChange {
            added: batches.to_vec(),
            ..TableChange::default()
        };
        overwrite_removed_ids(base, table, &change, system_columns, |id| {
            (admission.removed)(id)
        })
        .await?
    } else {
        Vec::new()
    };
    let mut reuse = admission.reuse.as_mut().and_then(|admit| admit());
    let mut cached_base = reuse.as_ref().map(|reuse| {
        let mut snapshot = base.clone();
        snapshot.set_read_caches(reuse.snapshot_caches());
        snapshot
    });
    let integer_endpoints = checks.iter().any(|spec| {
        matches!(&spec.check, ValidateCheck::EdgeRi { node_type, .. } if int_keyed(catalog, node_type))
    });
    let mut first = FirstViolation::default();
    let mut edge_ri_judged = false;
    let mut retained_judged: HashSet<&str> = HashSet::new();
    for (index, spec) in checks.iter().enumerate() {
        if matches!(spec.check, ValidateCheck::RetainedEdges { .. })
            || (integer_endpoints && matches!(spec.check, ValidateCheck::EdgeRi { .. }))
        {
            drop(cached_base.take());
            drop(reuse.take());
        }
        let committed = CommittedState {
            committed: Some(cached_base.as_ref().unwrap_or(base)),
            overwritten: overwrite.then(|| table.to_string()).into_iter().collect(),
            live: None,
        };
        match &spec.check {
            ValidateCheck::Value { .. } => first.refuse(extracted.value.take())?,
            ValidateCheck::Unique { columns, .. } => {
                let (image, repeat) = extracted
                    .unique
                    .remove(&index)
                    .ok_or_else(|| missing_facts(spec))?;
                first.refuse(repeat)?;
                unique_coalesced_dups(table, columns, &image.entries, &mut first.sink())?;
                first.refuse_pending()?;
                if spec.consults.is_some() {
                    let probe = unique_probe_keys(&image.entries);
                    let holders = committed
                        .unique_holders(table, columns, &probe, system_columns, access_path(spec)?)
                        .await?;
                    unique_committed_collisions(
                        table,
                        columns,
                        &image.entries,
                        &holders,
                        &extracted.ids,
                        &HashSet::new(),
                        &mut first.sink(),
                    )?;
                    first.refuse_pending()?;
                }
            }
            ValidateCheck::EdgeRi { .. } => {
                if edge_ri_judged || delta_rows == 0 {
                    continue;
                }
                edge_ri_judged = true;
                if spec.probe == Some(EndpointProbe::Swept) {
                    first.refuse(extracted.orphan.take())?;
                    continue;
                }
                let edge_type = edge_type_of(catalog, table)?;
                let access_of = |wanted: Endpoint| -> Result<AccessPath> {
                    let spec = checks
                        .iter()
                        .find(|spec| {
                            matches!(&spec.check, ValidateCheck::EdgeRi { endpoint, .. } if *endpoint == wanted)
                        })
                        .unwrap_or(spec);
                    access_path(spec)
                };
                let from_keys = EndpointKeys::load(
                    &committed,
                    catalog,
                    &edge_type.from_type,
                    batches,
                    system_columns.src,
                    system_columns,
                    access_of(Endpoint::From)?,
                    admission,
                )
                .await?;
                let to_keys = EndpointKeys::load(
                    &committed,
                    catalog,
                    &edge_type.to_type,
                    batches,
                    system_columns.dst,
                    system_columns,
                    access_of(Endpoint::To)?,
                    admission,
                )
                .await?;
                first_orphan(
                    table,
                    &edge_type.from_type,
                    &edge_type.to_type,
                    batches,
                    &from_keys,
                    &to_keys,
                    system_columns,
                    admission,
                    &mut first.sink(),
                )?;
                first.refuse_pending()?;
            }
            ValidateCheck::Cardinality { .. } => {
                let edge_type = edge_type_of(catalog, table)?;
                if edge_type.cardinality.is_default() {
                    continue;
                }
                let delta_by_id = std::mem::take(&mut extracted.card);
                let plan = CardPlan {
                    changed_ids: delta_by_id.keys().cloned().collect(),
                    delta_by_id,
                    removed_ids: Vec::new(),
                };
                let moved_from = committed
                    .committed_edges(table, system_columns.id, &plan.changed_ids, system_columns)
                    .await?;
                let affected = card_affected(&plan, &[], &moved_from, &HashSet::new());
                if affected.is_empty() {
                    continue;
                }
                let affected_ids: Vec<String> = affected.iter().cloned().collect();
                let committed_for_affected = committed
                    .committed_edges(table, system_columns.src, &affected_ids, system_columns)
                    .await?;
                card_violations(
                    table,
                    edge_type,
                    &plan,
                    &affected,
                    &committed_for_affected,
                    &mut first.sink(),
                )?;
                first.refuse_pending()?;
            }
            ValidateCheck::RetainedEdges { edge_table, .. } => {
                if !retained_judged.insert(edge_table) {
                    continue;
                }
                let edge_type = edge_type_of(catalog, edge_table)?;
                let deleted_at = |wanted: Endpoint| -> &[String] {
                    let listed = checks.iter().any(|spec| {
                        matches!(&spec.check, ValidateCheck::RetainedEdges { edge_table: table, endpoint }
                            if table == edge_table && *endpoint == wanted)
                    });
                    if listed { removed.as_slice() } else { &[] }
                };
                let deleted_from = deleted_at(Endpoint::From);
                let deleted_to = deleted_at(Endpoint::To);
                let retained = committed
                    .first_edge_referencing(
                        edge_table,
                        deleted_from,
                        deleted_to,
                        system_columns,
                        admission.retained,
                    )
                    .await?;
                orphans_retained(
                    edge_table,
                    &edge_type.from_type,
                    &edge_type.to_type,
                    &retained,
                    &HashSet::new(),
                    &HashSet::new(),
                    deleted_from,
                    deleted_to,
                    system_columns,
                    &mut first.sink(),
                )?;
                first.refuse_pending()?;
            }
        }
    }
    Ok(())
}

/// The position of a row: the slice key `partition << 32 | index` the
/// source tagged its batch with, and row `row` inside the slice, packed
/// `partition << 48 | index << 24 | row` so positions sort in source order.
pub(crate) fn position(slice: usize, row: u64) -> Position {
    let partition = (slice >> 32) as Position;
    let index = (slice & 0xff_ffff) as Position;
    (partition << 48) | (index << 24) | (row & 0xff_ffff)
}

/// What the sweep extracts, read off the checks once: which facts a
/// partition keeps, the shards its unique holders split into, and the
/// endpoints it probes when the plan sweeps the edge's endpoint checks.
pub(crate) struct SweepPlan {
    table: String,
    needs_value: bool,
    needs_ids: bool,
    needs_card: bool,
    unique: Vec<(usize, Vec<String>)>,
    /// Per uniqueness check, in `unique`'s order: the first holders every
    /// partition inserts into as it sweeps.
    unique_holders: Vec<Arc<SharedFirstHolders>>,
    input_only: bool,
    /// The swept endpoint checks, `From` before `To`, so a row whose src and
    /// dst both miss reports its src as [`first_orphan`] does.
    endpoints: Vec<SweptEndpoint>,
}

struct SweptEndpoint {
    endpoint: Endpoint,
    node_type: String,
}

impl SweepPlan {
    /// `shards` is the partitions that sweep at once; each uniqueness check's
    /// shared holders split into that many locked maps, so the partitions
    /// rarely wait on one another.
    pub(crate) fn new(
        table: &str,
        checks: &[ValidateSpec],
        overwrite: bool,
        shards: usize,
    ) -> Self {
        let unique: Vec<(usize, Vec<String>)> = checks
            .iter()
            .enumerate()
            .filter_map(|(index, spec)| match &spec.check {
                ValidateCheck::Unique { columns, .. } => Some((index, columns.clone())),
                _ => None,
            })
            .collect();
        let mut endpoints: Vec<SweptEndpoint> = checks
            .iter()
            .filter(|spec| spec.probe == Some(EndpointProbe::Swept))
            .filter_map(|spec| match &spec.check {
                ValidateCheck::EdgeRi {
                    endpoint,
                    node_type,
                } => Some(SweptEndpoint {
                    endpoint: *endpoint,
                    node_type: node_type.clone(),
                }),
                _ => None,
            })
            .collect();
        endpoints.sort_by_key(|swept| swept.endpoint != Endpoint::From);
        Self {
            table: table.to_string(),
            needs_value: checks
                .iter()
                .any(|spec| matches!(spec.check, ValidateCheck::Value { .. })),
            needs_ids: checks.iter().any(|spec| {
                matches!(spec.check, ValidateCheck::Unique { .. }) && spec.consults.is_some()
            }),
            needs_card: checks
                .iter()
                .any(|spec| matches!(spec.check, ValidateCheck::Cardinality { .. })),
            unique_holders: unique
                .iter()
                .map(|_| Arc::new(SharedFirstHolders::new(shards)))
                .collect(),
            unique,
            input_only: overwrite,
            endpoints,
        }
    }

    /// Whether the partitions probe the endpoint checks as they sweep.
    pub(crate) fn sweeps_endpoints(&self) -> bool {
        !self.endpoints.is_empty()
    }

    /// Every uniqueness check's holders sized for `rows` once, the first time
    /// a partition has the source's estimate; `admit` charges one check's
    /// table bytes and returns the charge, or `None` to let that table grow;
    /// `per_row_charged` is what the drain charges per row for the check,
    /// which carries the table from some row on.
    pub(crate) fn size_unique_holders(
        &self,
        rows: usize,
        per_row_charged: usize,
        admit: &mut dyn FnMut(usize) -> Option<Box<dyn Send>>,
    ) {
        for holders in &self.unique_holders {
            holders.size(rows, per_row_charged, &mut *admit);
        }
    }

    /// Every uniqueness check's holders taken out of their shards, one box per
    /// shard, for the caller to free on workers; the judgment has read them.
    pub(crate) fn release_unique_holders(&self) -> Vec<Box<dyn Send>> {
        self.unique_holders
            .iter()
            .flat_map(|holders| holders.take_maps())
            .collect()
    }
}

/// The committed integer key sets the swept endpoint checks probe, one per
/// node type, loaded beside the parse and shared by every partition.
pub(crate) struct SweptSets {
    sets: HashMap<String, IntKeySet>,
}

/// The bytes a partition's probe scratch grows by to parse `rows` more
/// integer keys per batch than it has room for.
pub(crate) fn parse_scratch_bytes(rows: usize) -> Result<usize> {
    key_bytes(rows).ok_or_else(|| integer_capacity_error(rows))
}

/// The sets the plan's swept endpoint checks probe, admitted but not yet
/// read: one committed table per distinct endpoint node type with its row
/// count, the judged route's read (`CommittedState::existing_int_keys` under
/// `Scan`) once per table instead of once per endpoint. Every byte of the
/// read is admitted here, before the parse starts, and the scan and build
/// scratch stay admitted in `transient` until the caller releases them after
/// the parse, so the loader never moves the pool while a partition charges
/// it: a refusal's `actual` replays the same under DST.
pub(crate) struct AdmittedSets {
    tables: Vec<(String, Option<Dataset>, usize)>,
    retained: Vec<Box<dyn Send>>,
    transient: Vec<Box<dyn Send>>,
}

/// Admit the swept endpoint key sets of `table`'s load: the input, retained
/// and build scratch bytes of each distinct endpoint table's row count.
pub(crate) async fn admit_swept_sets(
    table: &str,
    checks: &[ValidateSpec],
    base: &Snapshot,
    overwrite: bool,
    admit: &mut ByteAdmission<'_>,
) -> Result<AdmittedSets> {
    let committed = CommittedState {
        committed: Some(base),
        overwritten: overwrite.then(|| table.to_string()).into_iter().collect(),
        live: None,
    };
    let mut admitted = AdmittedSets {
        tables: Vec::new(),
        retained: Vec::new(),
        transient: Vec::new(),
    };
    for spec in checks {
        let ValidateCheck::EdgeRi { node_type, .. } = &spec.check else {
            continue;
        };
        if spec.probe != Some(EndpointProbe::Swept)
            || admitted
                .tables
                .iter()
                .any(|(table, _, _)| table == node_type)
        {
            continue;
        }
        let ds = committed.open(&format!("node:{node_type}")).await?;
        let rows = match &ds {
            Some(ds) => ds.count_rows(None).await.map_err(OmniError::storage)?,
            None => 0,
        };
        let layout = IntKeySetLayout::new(rows).ok_or_else(|| integer_capacity_error(rows))?;
        admitted.transient.push(admit(
            key_bytes(rows).ok_or_else(|| integer_capacity_error(rows))?,
        )?);
        admitted.retained.push(admit(layout.retained_bytes)?);
        admitted.transient.push(admit(layout.scratch_bytes)?);
        admitted.tables.push((node_type.clone(), ds, rows));
    }
    Ok(admitted)
}

impl AdmittedSets {
    /// Read and build every admitted set; the retained guards hold the sets'
    /// bytes, the transient ones the scan input and build scratch.
    pub(crate) async fn load(
        self,
        system_columns: SystemColumns,
    ) -> Result<(SweptSets, Vec<Box<dyn Send>>, Vec<Box<dyn Send>>)> {
        let mut sets = HashMap::new();
        for (node_type, ds, bound) in self.tables {
            let mut keys = Vec::with_capacity(bound);
            if let Some(ds) = ds.filter(|_| bound > 0) {
                scan_int_ids(&ds, system_columns.id, &mut keys, bound).await?;
            }
            let layout = IntKeySetLayout::new(keys.len())
                .ok_or_else(|| integer_capacity_error(keys.len()))?;
            sets.insert(node_type, IntKeySet::build(&keys, layout));
        }
        Ok((SweptSets { sets }, self.retained, self.transient))
    }
}

/// The facts one partition extracted from the batches it drained, every row
/// placed by its [`Position`]; [`Extracted::prepare`] joins the partitions'.
pub(crate) struct SweepPartial {
    /// Per value constraint, by its declaration index: the earliest row that
    /// violates it, so the merge names constraints in declaration order over
    /// the whole source, whatever batch or slice each violation sits in.
    value: Vec<Option<(Position, Violation)>>,
    ids: HashSet<String>,
    unique: Vec<UniquePartial>,
    card: HashMap<String, (Position, String)>,
    /// The earliest endpoint miss among the partition's rows, when the plan
    /// sweeps the endpoint checks.
    orphan: Option<(Position, Violation)>,
    parsed: Vec<i64>,
}

impl SweepPartial {
    pub(crate) fn new(plan: &SweepPlan) -> Self {
        Self {
            value: Vec::new(),
            ids: HashSet::new(),
            unique: plan
                .unique_holders
                .iter()
                .map(|holders| UniquePartial::new(plan.input_only, Arc::clone(holders)))
                .collect(),
            card: HashMap::new(),
            orphan: None,
            parsed: Vec::new(),
        }
    }

    /// Probe `batch`'s swept endpoints against `sets`, keeping the earliest
    /// miss: row order, src before dst within a row, as [`first_orphan`]
    /// reports it over the whole delta. `base` is the batch's first row.
    pub(crate) fn probe_endpoints(
        &mut self,
        plan: &SweepPlan,
        batch: &RecordBatch,
        system_columns: SystemColumns,
        base: Position,
        sets: &SweptSets,
    ) -> Result<()> {
        let mut miss: Option<(Position, Violation)> = None;
        for swept in &plan.endpoints {
            let column = match swept.endpoint {
                Endpoint::From => system_columns.src,
                Endpoint::To => system_columns.dst,
            };
            let keys = sets.sets.get(&swept.node_type).ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "swept endpoint check on {} has no loaded key set",
                    swept.node_type
                ))
            })?;
            let values = string_col(batch, column)?;
            let Some(row) = first_int_miss(keys, values, &mut self.parsed) else {
                continue;
            };
            let position = base.saturating_add(row as Position);
            if miss.as_ref().is_none_or(|(at, _)| position < *at) {
                let ids = string_col(batch, system_columns.id)?;
                miss = Some((
                    position,
                    orphan_violation(
                        &plan.table,
                        ids.value(row),
                        column,
                        values.value(row),
                        &swept.node_type,
                    ),
                ));
            }
        }
        if let Some((position, violation)) = miss
            && self.orphan.as_ref().is_none_or(|(at, _)| position < *at)
        {
            self.orphan = Some((position, violation));
        }
        Ok(())
    }

    /// Extract `batch`, whose first row sits at `base`; the partition's
    /// batches arrive in source order, so within one partial later wins.
    pub(crate) fn observe(
        &mut self,
        plan: &SweepPlan,
        catalog: &Catalog,
        batch: &RecordBatch,
        system_columns: SystemColumns,
        base: Position,
    ) -> Result<()> {
        let table = plan.table.as_str();
        if plan.needs_value {
            let type_name = table.split_once(':').map(|(_, name)| name).unwrap_or(table);
            for (index, row, error) in value_check(table, type_name, catalog, batch) {
                if index >= self.value.len() {
                    self.value.resize(index + 1, None);
                }
                if self.value[index].is_none() {
                    let position = base.saturating_add(row as Position);
                    self.value[index] = Some((position, value_violation(table, error)));
                }
            }
        }
        if plan.needs_ids {
            let ids = string_col(batch, system_columns.id)?;
            for row in 0..ids.len() {
                if !ids.is_null(row) {
                    self.ids.insert(ids.value(row).to_string());
                }
            }
        }
        for ((_, columns), partial) in plan.unique.iter().zip(&mut self.unique) {
            partial.observe(table, columns, batch, system_columns, base)?;
        }
        if plan.needs_card {
            let ids = string_col(batch, system_columns.id)?;
            let srcs = string_col(batch, system_columns.src)?;
            for row in 0..batch.num_rows() {
                self.card.insert(
                    ids.value(row).to_string(),
                    (
                        base.saturating_add(row as Position),
                        srcs.value(row).to_string(),
                    ),
                );
            }
        }
        Ok(())
    }
}

/// What one sweep over the delta extracts for every check at once.
#[derive(Default)]
pub(crate) struct Extracted {
    /// The first value violation: the first constraint in declaration order
    /// with a violating row anywhere in the delta, at its earliest row.
    value: Option<Violation>,
    /// Every non-null id of the delta.
    ids: HashSet<String>,
    /// Per uniqueness check (by list index): its image and the first key
    /// repeated anywhere in the input.
    unique: HashMap<usize, (UniqueImage, Option<Violation>)>,
    /// Last-wins src per edge id, when a cardinality check reads it.
    card: HashMap<String, String>,
    /// The first endpoint miss of the delta, when the plan swept the
    /// endpoint checks on the partitions.
    orphan: Option<Violation>,
}

impl Extracted {
    /// Every batch swept in order as one partial, merged alone.
    #[cfg(test)]
    fn sweep(
        table: &str,
        checks: &[ValidateSpec],
        catalog: &Catalog,
        batches: &[RecordBatch],
        system_columns: SystemColumns,
        overwrite: bool,
    ) -> Result<Self> {
        let plan = SweepPlan::new(table, checks, overwrite, 1);
        let mut partial = SweepPartial::new(&plan);
        let mut base = 0;
        for batch in batches {
            partial.observe(&plan, catalog, batch, system_columns, base)?;
            base += batch.num_rows() as Position;
        }
        Ok(Self::prepare(&plan, vec![partial]))
    }

    /// Join the partitions' partials: the first value violation (the earliest
    /// row per constraint, then the first constraint in declaration order),
    /// the union of ids, the last src per edge id, each check's final keys
    /// merged into its image and its earliest repeat read from the shared
    /// holders, which the partitions judged as they swept.
    pub(crate) fn prepare(plan: &SweepPlan, partials: Vec<SweepPartial>) -> Self {
        let mut value: Vec<Option<(Position, Violation)>> = Vec::new();
        let mut ids = HashSet::new();
        let mut card: HashMap<String, (Position, String)> = HashMap::new();
        let mut finals: Vec<Vec<FinalKeys>> = plan.unique.iter().map(|_| Vec::new()).collect();
        let mut orphan: Option<(Position, Violation)> = None;
        for partial in partials {
            if let Some((position, violation)) = partial.orphan
                && orphan.as_ref().is_none_or(|(at, _)| position < *at)
            {
                orphan = Some((position, violation));
            }
            if partial.value.len() > value.len() {
                value.resize(partial.value.len(), None);
            }
            for (index, found) in partial.value.into_iter().enumerate() {
                if let Some((position, violation)) = found {
                    if value[index].as_ref().is_none_or(|(at, _)| position < *at) {
                        value[index] = Some((position, violation));
                    }
                }
            }
            ids.extend(partial.ids);
            for (id, (position, src)) in partial.card {
                match card.get(&id) {
                    Some((held, _)) if *held > position => {}
                    _ => {
                        card.insert(id, (position, src));
                    }
                }
            }
            for (index, unique_partial) in partial.unique.into_iter().enumerate() {
                finals[index].push(unique_partial.into_final_keys());
            }
        }
        let unique = plan
            .unique
            .iter()
            .zip(finals)
            .zip(&plan.unique_holders)
            .map(|(((check, _), finals), holders)| {
                let repeat = holders.first_repeat().map(|(_, violation)| violation);
                let image = merge_unique_image(finals, repeat.is_some());
                (*check, (image, repeat))
            })
            .collect();
        Self {
            value: value
                .into_iter()
                .flatten()
                .next()
                .map(|(_, violation)| violation),
            ids,
            unique,
            card: card.into_iter().map(|(id, (_, src))| (id, src)).collect(),
            orphan: orphan.map(|(_, violation)| violation),
        }
    }
}

/// The evaluator's row-local value judgment of one batch: every violated
/// constraint with its declaration index and first violating row, `@range`
/// and `@check` then enum membership on a node, enum membership on an edge.
fn value_check(
    table: &str,
    type_name: &str,
    catalog: &Catalog,
    batch: &RecordBatch,
) -> Vec<(usize, usize, OmniError)> {
    if table.starts_with("node:") {
        let Some(node_type) = catalog.node_types.get(type_name) else {
            return Vec::new();
        };
        let declared = node_type.range_constraints.len() + node_type.check_constraints.len();
        let mut failures = value_constraint_failures(batch, node_type);
        failures.extend(
            enum_constraint_failures(batch, &node_type.properties, type_name)
                .into_iter()
                .map(|(index, row, error)| (declared + index, row, error)),
        );
        failures
    } else {
        match catalog.edge_types.get(type_name) {
            Some(edge_type) => enum_constraint_failures(batch, &edge_type.properties, type_name),
            None => Vec::new(),
        }
    }
}

/// The first violation a finalizer reports, kept until the check is settled.
#[derive(Default)]
struct FirstViolation {
    pending: Option<Violation>,
}

impl FirstViolation {
    fn sink(&mut self) -> impl FnMut(Violation) -> Result<()> + '_ {
        move |violation| {
            self.pending.get_or_insert(violation);
            Ok(())
        }
    }

    fn refuse_pending(&mut self) -> Result<()> {
        let pending = self.pending.take();
        self.refuse(pending)
    }

    fn refuse(&mut self, violation: Option<Violation>) -> Result<()> {
        match violation {
            Some(violation) => Err(violation.into_omni_error()),
            None => Ok(()),
        }
    }
}

fn access_path(spec: &ValidateSpec) -> Result<AccessPath> {
    match spec.access {
        Some(ValidateAccess::Probe) => Ok(AccessPath::Probe),
        Some(ValidateAccess::Scan) => Ok(AccessPath::Scan),
        other => Err(OmniError::manifest_internal(format!(
            "validate check {} consults a committed image under access {other:?}",
            spec.check.name()
        ))),
    }
}

fn missing_facts(spec: &ValidateSpec) -> OmniError {
    OmniError::manifest_internal(format!(
        "validate check {} has no extracted facts",
        spec.check.name()
    ))
}

/// The committed ids of one endpoint table, as the probe reads them: an
/// integer key set when the node type's `@key` is one `I32`/`I64`/`U32`
/// property (its ids are canonical decimals), the id strings otherwise.
enum EndpointKeys {
    Int {
        keys: IntKeySet,
        _memory: Box<dyn Send>,
    },
    Text(HashSet<String>),
}

impl EndpointKeys {
    async fn load(
        committed: &CommittedState<'_>,
        catalog: &Catalog,
        node_type: &str,
        batches: &[RecordBatch],
        column: &str,
        system_columns: SystemColumns,
        access: AccessPath,
        admission: &mut LoadAdmission<'_>,
    ) -> Result<Self> {
        let table_key = format!("node:{node_type}");
        let _wanted_memory = match access {
            AccessPath::Probe => admission.reserve_bytes(endpoint_value_bytes(batches, column)?)?,
            AccessPath::Scan => Box::new(()),
        };
        if int_keyed(catalog, node_type) {
            let wanted = match access {
                AccessPath::Probe => endpoint_values(batches, column)?,
                AccessPath::Scan => Vec::new(),
            };
            let (keys, memory) = committed
                .existing_int_keys(&table_key, &wanted, system_columns, access, &mut |bytes| {
                    admission.reserve_bytes(bytes)
                })
                .await?;
            return Ok(Self::Int {
                keys,
                _memory: memory,
            });
        }
        let values = endpoint_values(batches, column)?;
        let ids = committed
            .existing_ids(&table_key, &values, system_columns, access)
            .await?;
        Ok(Self::Text(ids))
    }

    /// The first row of `column` naming no committed id (a null or a
    /// non-canonical spelling misses). Integer keys are parsed for the whole
    /// column into `parsed` first, so the probes run as independent loads.
    fn first_miss(&self, column: &StringArray, parsed: &mut Vec<i64>) -> Option<usize> {
        match self {
            Self::Int { keys, .. } => first_int_miss(keys, column, parsed),
            Self::Text(ids) => (0..column.len())
                .find(|&row| column.is_null(row) || !ids.contains(column.value(row))),
        }
    }
}

/// The first row of `column` naming no committed integer key: a null or a
/// non-canonical spelling misses. The column is parsed into `parsed` first,
/// so the probes run as independent loads.
fn first_int_miss(keys: &IntKeySet, column: &StringArray, parsed: &mut Vec<i64>) -> Option<usize> {
    parsed.clear();
    let mut unparsed = None;
    for row in 0..column.len() {
        let key = (!column.is_null(row))
            .then(|| canonical_i64(column.value(row)))
            .flatten();
        match key {
            Some(key) => parsed.push(key),
            None => {
                unparsed = Some(row);
                break;
            }
        }
    }
    parsed
        .iter()
        .position(|&key| !keys.contains(key))
        .or(unparsed)
}

pub(crate) fn int_keyed(catalog: &Catalog, node_type: &str) -> bool {
    catalog
        .node_types
        .get(node_type)
        .and_then(|node| match node.key.as_deref() {
            Some([key]) => node.properties.get(key),
            _ => None,
        })
        .is_some_and(|prop| {
            !prop.list
                && matches!(
                    prop.scalar,
                    ScalarType::I32 | ScalarType::I64 | ScalarType::U32
                )
        })
}

/// What `endpoint_values` allocates: one `String` per cell plus its text.
fn endpoint_value_bytes(batches: &[RecordBatch], column: &str) -> Result<usize> {
    let mut bytes = 0usize;
    for batch in batches {
        let endpoints = string_col(batch, column)?;
        let text = endpoints
            .iter()
            .flatten()
            .fold(0usize, |sum, value| sum.saturating_add(value.len()));
        bytes = bytes
            .saturating_add(text)
            .saturating_add(endpoints.len().saturating_mul(size_of::<String>()));
    }
    Ok(bytes)
}

fn endpoint_values(batches: &[RecordBatch], column: &str) -> Result<Vec<String>> {
    let mut values = Vec::new();
    for batch in batches {
        let endpoints = string_col(batch, column)?;
        values.extend((0..endpoints.len()).map(|row| endpoints.value(row).to_string()));
    }
    Ok(values)
}

/// The first delta edge, in row order and src before dst, whose endpoint is
/// absent from its node table: the one violation the endpoint check reports.
fn first_orphan<F>(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    batches: &[RecordBatch],
    from_keys: &EndpointKeys,
    to_keys: &EndpointKeys,
    system_columns: SystemColumns,
    admission: &mut LoadAdmission<'_>,
    sink: &mut F,
) -> Result<()>
where
    F: FnMut(Violation) -> Result<()>,
{
    let parsed_len = if matches!(from_keys, EndpointKeys::Int { .. })
        || matches!(to_keys, EndpointKeys::Int { .. })
    {
        batches.iter().map(RecordBatch::num_rows).max().unwrap_or(0)
    } else {
        0
    };
    let _parsed_memory = admission
        .reserve_bytes(key_bytes(parsed_len).ok_or_else(|| integer_capacity_error(parsed_len))?)?;
    let mut parsed = Vec::with_capacity(parsed_len);
    for batch in batches {
        let ids = string_col(batch, system_columns.id)?;
        let srcs = string_col(batch, system_columns.src)?;
        let dsts = string_col(batch, system_columns.dst)?;
        let src_miss = from_keys.first_miss(srcs, &mut parsed);
        let dst_miss = to_keys.first_miss(dsts, &mut parsed);
        let (row, column, values, node_type) = match (src_miss, dst_miss) {
            (None, None) => continue,
            (Some(src), Some(dst)) if dst < src => (dst, system_columns.dst, dsts, to_type),
            (Some(src), _) => (src, system_columns.src, srcs, from_type),
            (None, Some(dst)) => (dst, system_columns.dst, dsts, to_type),
        };
        return sink(orphan_violation(
            edge_table,
            ids.value(row),
            column,
            values.value(row),
            node_type,
        ));
    }
    Ok(())
}

/// A plan omits the removal scan only when nothing can read removed ids: no
/// retained-edge check is planned, and a referenced node target holds no
/// committed rows; a manifest count the dataset contradicts refuses the load.
async fn removal_omitted_soundly(
    table: &str,
    checks: &[ValidateSpec],
    catalog: &Catalog,
    base: &Snapshot,
) -> Result<()> {
    if checks
        .iter()
        .any(|spec| matches!(spec.check, ValidateCheck::RetainedEdges { .. }))
    {
        return Err(OmniError::manifest_internal(format!(
            "Validate: the plan omits the removal scan of {table} and reads removed ids in a retained-edge check"
        )));
    }
    let Some(name) = table.strip_prefix("node:") else {
        return Ok(());
    };
    let referenced = catalog
        .edge_types
        .values()
        .any(|edge| edge.from_type == name || edge.to_type == name);
    if !referenced || base.dataset(table).is_none() {
        return Ok(());
    }
    let ds = base.open_lance_dataset(table).await?;
    let rows = ds.count_rows(None).await.map_err(OmniError::storage)?;
    if rows == 0 {
        return Ok(());
    }
    Err(OmniError::manifest_internal(format!(
        "Validate: the plan omits the removal scan of {table} but the committed table holds {rows} rows"
    )))
}

fn edge_type_of<'c>(catalog: &'c Catalog, edge_table: &str) -> Result<&'c EdgeType> {
    edge_table
        .strip_prefix("edge:")
        .and_then(|name| catalog.edge_types.get(name))
        .ok_or_else(|| OmniError::manifest(format!("load csv: unknown edge table '{edge_table}'")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::StringArray;
    use arrow_schema::{DataType, Field, Schema};
    use omnigraph_compiler::ir::lower_load;
    use omnigraph_compiler::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};
    use omnigraph_compiler::settings::SessionSettings;
    use omnigraph_planner::ConsultedImage;

    use super::*;
    use crate::Session;
    use crate::db::Omnigraph;

    const GRAPH_SCHEMA: &str = "node Person {\n  name: String @key\n  email: String @unique\n}\nedge Knows: Person -> Person @card(0..2)\n";

    /// The slice key the source tags its batches with (`csv_source.rs`).
    fn slice_key(partition: usize, index: usize) -> usize {
        (partition << 32) | index
    }

    #[test]
    fn positions_order_partitions_before_slices_before_rows() {
        let p0_last_slice = position(slice_key(0, 7), 1_000_000);
        let p1_first_slice = position(slice_key(1, 0), 0);
        assert!(position(slice_key(0, 0), 5) < position(slice_key(0, 1), 0));
        assert!(p0_last_slice < p1_first_slice);
        assert!(p1_first_slice < position(slice_key(1, 0), 1));
        assert!(position(slice_key(1, 6), 9) < position(slice_key(2, 0), 0));
        assert_eq!(
            p1_first_slice.saturating_add(3),
            position(slice_key(1, 0), 3)
        );
    }

    const COMMITTED: &str = r#"{"type": "Person", "data": {"name": "Alice", "email": "alice@x"}}
{"type": "Person", "data": {"name": "Bob", "email": "bob@x"}}
{"type": "Person", "data": {"name": "Carol", "email": "carol@x"}}
{"edge": "Knows", "from": "Alice", "to": "Bob"}
{"edge": "Knows", "from": "Alice", "to": "Carol"}
"#;

    async fn committed_graph(dir: &tempfile::TempDir) -> Session {
        let db = Session::from_defaults(
            Arc::new(
                Omnigraph::init(dir.path().to_str().unwrap(), GRAPH_SCHEMA)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        db.load_jsonl(COMMITTED, LoadMode::Overwrite).await.unwrap();
        db
    }

    /// The physical checks of a load statement, every consulting check under
    /// `access`.
    fn checks(
        catalog: &Catalog,
        target: LoadTarget,
        mode: LoadStmtMode,
        access: ValidateAccess,
    ) -> Vec<ValidateSpec> {
        let stmt = LoadStmt {
            source: "file:///input.csv".to_string(),
            target,
            mode,
            delimiter: b',',
            header: true,
            columns: None,
        };
        lower_load(catalog, &stmt)
            .checks
            .into_iter()
            .map(|check| ValidateSpec {
                access: Some(if check.consults.is_some() {
                    access
                } else {
                    ValidateAccess::Delta
                }),
                consults: check.consults.map(|table| ConsultedImage {
                    table,
                    version: None,
                }),
                check: check.check,
                probe: None,
            })
            .collect()
    }

    fn load_mode(mode: LoadStmtMode) -> LoadMode {
        match mode {
            LoadStmtMode::Overwrite => LoadMode::Overwrite,
            LoadStmtMode::Append => LoadMode::Append,
            LoadStmtMode::Merge => LoadMode::Merge,
        }
    }

    fn person_batch(sc: SystemColumns, rows: &[(&str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(sc.id, DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("email", DataType::Utf8, false),
        ]));
        let names: Vec<&str> = rows.iter().map(|(name, _)| *name).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(names.clone())) as _,
                Arc::new(StringArray::from(names)) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, email)| *email).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    fn knows_batch(sc: SystemColumns, rows: &[(&str, &str, &str)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(sc.id, DataType::Utf8, false),
            Field::new(sc.src, DataType::Utf8, false),
            Field::new(sc.dst, DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, src, _)| *src).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, _, dst)| *dst).collect::<Vec<_>>(),
                )) as _,
            ],
        )
        .unwrap()
    }

    async fn judge(
        db: &Session,
        target: LoadTarget,
        mode: LoadStmtMode,
        access: ValidateAccess,
        batches: &[RecordBatch],
    ) -> std::result::Result<(), String> {
        let catalog = db.catalog();
        let snapshot = db.snapshot().await;
        let table = target.table_key();
        let checks = checks(&catalog, target, mode, access);
        judge_load(
            &table,
            &checks,
            &catalog,
            &snapshot,
            load_mode(mode),
            batches,
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |_| Ok(()),
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .map_err(|error| error.to_string())
    }

    /// Check order beats row order: a uniqueness repeat in the last batch is
    /// reported before an orphan endpoint in the first, under both accesses;
    /// a `.gqt` case sees one refusal text and cannot vary the access.
    #[tokio::test]
    async fn edge_append_refuses_in_check_order_under_probe_and_scan() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let sc = db.catalog().system_columns;
        for access in [ValidateAccess::Probe, ValidateAccess::Scan] {
            let orphan_first = [
                knows_batch(sc, &[("e1", "Bob", "Nobody"), ("e2", "Bob", "Carol")]),
                knows_batch(sc, &[("e3", "Ghost", "Alice")]),
            ];
            let refused = judge(
                &db,
                LoadTarget::Edge("Knows".into()),
                LoadStmtMode::Append,
                access,
                &orphan_first,
            )
            .await
            .unwrap_err();
            assert_eq!(
                refused,
                format!("{} 'Nobody' not found in Person", sc.dst),
                "{access:?}"
            );

            let over_card = [
                knows_batch(sc, &[("e1", "Alice", "Carol")]),
                knows_batch(sc, &[("e2", "Alice", "Bob")]),
            ];
            let refused = judge(
                &db,
                LoadTarget::Edge("Knows".into()),
                LoadStmtMode::Append,
                access,
                &over_card,
            )
            .await
            .unwrap_err();
            assert_eq!(
                refused, "@card violation on edge Knows: source 'Alice' has 4 edges (max 2)",
                "{access:?}: two committed and two new edges under one src"
            );

            let admitted = [knows_batch(
                sc,
                &[("e1", "Bob", "Carol"), ("e2", "Carol", "Bob")],
            )];
            judge(
                &db,
                LoadTarget::Edge("Knows".into()),
                LoadStmtMode::Append,
                access,
                &admitted,
            )
            .await
            .unwrap();
        }
    }

    /// A node append with a committed `@unique` holder in one batch and a
    /// `@key` repeat across batches: the key check precedes the unique one.
    #[tokio::test]
    async fn node_append_reports_the_key_repeat_before_the_committed_unique_holder() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let sc = db.catalog().system_columns;
        let batches = [
            person_batch(sc, &[("Zed", "alice@x")]),
            person_batch(sc, &[("Yan", "yan@x"), ("Yan", "yan2@x")]),
        ];
        let refused = judge(
            &db,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Append,
            ValidateAccess::Probe,
            &batches,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused,
            "@unique violation on Person.name: value 'Yan' held by 'Yan' and 'Yan'"
        );
        let only_committed = [person_batch(sc, &[("Zed", "alice@x")])];
        let refused = judge(
            &db,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Append,
            ValidateAccess::Scan,
            &only_committed,
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused,
            "@unique violation on Person.email: value 'alice@x' held by 'Alice' and 'Zed'"
        );
    }

    /// A node overwrite dropping a committed node reports the committed edge
    /// that would survive without it, admits the image keeping every
    /// referenced node, and charges each removed id through the callback once.
    #[tokio::test]
    async fn node_overwrite_reports_a_retained_edge_to_a_removed_node() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let catalog = db.catalog();
        let sc = catalog.system_columns;
        let snapshot = db.snapshot().await;
        let checks = checks(
            &catalog,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Overwrite,
            ValidateAccess::Scan,
        );
        assert_eq!(
            checks
                .iter()
                .map(|spec| spec.check.name())
                .collect::<Vec<_>>(),
            [
                "value",
                "unique",
                "unique",
                "retained_edges",
                "retained_edges"
            ]
        );
        let mut admitted = Vec::new();
        let refused = judge_load(
            "node:Person",
            &checks,
            &catalog,
            &snapshot,
            LoadMode::Overwrite,
            &[person_batch(sc, &[("Alice", "alice@x"), ("Bob", "bob@x")])],
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |id| {
                    admitted.push(id.to_string());
                    Ok(())
                },
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .unwrap_err();
        assert_eq!(
            refused.to_string(),
            format!("{} 'Carol' not found in Person", sc.dst)
        );
        assert_eq!(admitted, ["Carol"]);

        judge_load(
            "node:Person",
            &checks,
            &catalog,
            &snapshot,
            LoadMode::Overwrite,
            &[person_batch(
                sc,
                &[("Alice", "alice@x"), ("Bob", "bob@x"), ("Carol", "carol@x")],
            )],
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |_| Ok(()),
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .unwrap();
    }

    /// A plan that omits the removal scan is refused when a retained-edge
    /// check still reads removed ids, and when the referenced node target
    /// holds committed rows: the engine never trusts `none` on its own.
    #[tokio::test]
    async fn a_none_plan_refuses_a_listed_retained_edge_check_and_a_populated_target() {
        let dir = tempfile::tempdir().unwrap();
        let db = committed_graph(&dir).await;
        let catalog = db.catalog();
        let sc = catalog.system_columns;
        let snapshot = db.snapshot().await;
        let checks = checks(
            &catalog,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Overwrite,
            ValidateAccess::Scan,
        );
        let batches = [person_batch(
            sc,
            &[("Alice", "alice@x"), ("Bob", "bob@x"), ("Carol", "carol@x")],
        )];
        let listed = judge_load_swept(
            "node:Person",
            &checks,
            ValidateRemoval::None,
            &catalog,
            &snapshot,
            LoadMode::Overwrite,
            &batches,
            3,
            Extracted::sweep("node:Person", &checks, &catalog, &batches, sc, true).unwrap(),
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |_| Ok(()),
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .unwrap_err();
        assert!(
            listed.to_string().contains(
                "omits the removal scan of node:Person and reads removed ids in a retained-edge check"
            ),
            "{listed}"
        );

        let pruned: Vec<ValidateSpec> = checks
            .iter()
            .filter(|spec| !matches!(spec.check, ValidateCheck::RetainedEdges { .. }))
            .cloned()
            .collect();
        let populated = judge_load_swept(
            "node:Person",
            &pruned,
            ValidateRemoval::None,
            &catalog,
            &snapshot,
            LoadMode::Overwrite,
            &batches,
            3,
            Extracted::sweep("node:Person", &pruned, &catalog, &batches, sc, true).unwrap(),
            &mut LoadAdmission {
                phase: None,
                bytes: None,
                reuse: None,
                removed: &mut |_| Ok(()),
                retained: &mut |_| Ok(()),
            },
        )
        .await
        .unwrap_err();
        assert!(
            populated
                .to_string()
                .contains("omits the removal scan of node:Person but the committed table holds"),
            "{populated}"
        );
    }
}
