//! The one-pass judgment of a write plan's `Validate` nodes: every check's
//! facts are extracted from each delta batch once while it is hot, then the
//! checks are judged after the last batch, every table's value check first
//! and then each table's remaining checks in its node's order (the refusal
//! precedence), each consulting the committed image under its physical
//! access. Every kept-table write runs here: the `load csv` operator sweeps
//! its partitions into the judge, the staged routes (a JSONL load, a
//! mutation, a blob cell, a branch merge) hand it their batches whole.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{Array, RecordBatch, StringArray};
use lance::Dataset;
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::catalog::{Catalog, EdgeType};
use omnigraph_compiler::ir::{DeltaClass, Endpoint, ValidateCheck, WriteKind};
use omnigraph_compiler::types::ScalarType;
use omnigraph_planner::{EndpointProbe, ValidateAccess, ValidateRemoval, ValidateSpec};

use super::kernels::{
    CardPlan, FinalKeys, Position, Repeats, SharedFirstHolders, UniqueImage, UniquePartial,
    merge_unique_image, orphan_violation, orphans_retained, unique_coalesced_dups,
    unique_committed_collisions, unique_probe_keys,
};
use super::key_set::{IntKeySet, IntKeySetLayout, canonical_i64, key_bytes};
use super::{
    AccessPath, CommittedState, Violation, integer_capacity_error, overwrite_removed_ids,
    scan_int_ids, string_col, value_violation,
};
use crate::db::{Omnigraph, Snapshot};
use crate::error::{OmniError, Result};
use crate::loader::read_reuse::LoadReadReuse;
use crate::loader::{enum_constraint_failures, value_constraint_failures};
use crate::storage_layer::WriteBudget;

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

/// How a write reports: a load or a mutation refuses on the first violation
/// in check order, with the violation's text as the error; a branch merge
/// lists every violation with its kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Report {
    First,
    Every,
}

/// Which repeats of a unique key inside the delta are violations. A load's
/// input has no row order, so a key repeated anywhere in it is one; a
/// mutation's batches are its statements in order, so a later statement may
/// move a row's key and free the value, and a repeat is judged inside one
/// batch and then on the rows' final keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RepeatRule {
    AcrossInput,
    ByFinalKey,
}

impl RepeatRule {
    pub(crate) fn for_write(kind: WriteKind) -> Self {
        match kind {
            WriteKind::Load => Self::AcrossInput,
            WriteKind::Mutation | WriteKind::BlobCell | WriteKind::BranchMerge => Self::ByFinalKey,
        }
    }
}

/// The committed image a write's lookups read: the transaction's pinned
/// base, the merge target, or the base with `@card` counted on the live
/// branch (#298).
pub(crate) enum JudgedSide {
    Base(Snapshot),
    MergeTarget(Snapshot),
    Live {
        base: Snapshot,
        branch: Option<String>,
    },
}

impl JudgedSide {
    pub(crate) fn snapshot(&self) -> &Snapshot {
        match self {
            Self::Base(snapshot) | Self::MergeTarget(snapshot) => snapshot,
            Self::Live { base, .. } => base,
        }
    }

    fn live<'a>(&'a self, db: &'a Omnigraph) -> Option<(&'a Omnigraph, Option<&'a str>)> {
        match self {
            Self::Live { branch, .. } => Some((db, branch.as_deref())),
            Self::Base(_) | Self::MergeTarget(_) => None,
        }
    }
}

/// One table of a write as its `Validate` node decided it, plus the ids the
/// write removes from the table, which the node's checks exempt and recount.
pub(crate) struct JudgedTable {
    pub(crate) table: String,
    pub(crate) checks: Vec<ValidateSpec>,
    pub(crate) removal: ValidateRemoval,
    pub(crate) delta: DeltaClass,
    pub(crate) deleted_ids: Vec<String>,
}

/// What one write's validation judges against: every table's plan node, the
/// committed image the transaction pins, and how violations are reported.
pub(crate) struct WriteJudge {
    pub(crate) tables: Vec<JudgedTable>,
    pub(crate) committed: JudgedSide,
    pub(crate) write_budget: WriteBudget,
    pub(crate) catalog: Arc<Catalog>,
    pub(crate) report: Report,
    pub(crate) repeat: RepeatRule,
}

impl WriteJudge {
    /// The one table of a `load csv` plan's judge.
    pub(crate) fn only(&self) -> &JudgedTable {
        &self.tables[0]
    }

    pub(crate) fn snapshot(&self) -> &Snapshot {
        self.committed.snapshot()
    }

    fn replaced(&self) -> HashSet<String> {
        self.tables
            .iter()
            .filter(|table| table.delta.replaces())
            .map(|table| table.table.clone())
            .collect()
    }

    fn table_index(&self, table: &str) -> Option<usize> {
        self.tables.iter().position(|judged| judged.table == table)
    }

    /// Whether the sweep of table `index` keeps every id: a consulted
    /// uniqueness or a source cardinality check of its own reads them, or a
    /// check of another table in the same write settles against them (an
    /// endpoint of a delta edge, a retained edge the write itself re-places,
    /// a written source under a `@card` minimum).
    pub(crate) fn needs_ids(&self, index: usize) -> bool {
        let table = &self.tables[index];
        let own = table.checks.iter().any(|spec| match spec.check {
            ValidateCheck::Unique { .. } => spec.consults.is_some(),
            ValidateCheck::SourceCardinality { .. } => true,
            _ => false,
        });
        own || self
            .tables
            .iter()
            .enumerate()
            .filter(|(other, _)| *other != index)
            .any(|(_, other)| {
                other.checks.iter().any(|spec| match &spec.check {
                    ValidateCheck::EdgeRi { node_type, .. } => {
                        table.table == format!("node:{node_type}")
                    }
                    ValidateCheck::RetainedEdges { edge_table, .. } => table.table == *edge_table,
                    ValidateCheck::Cardinality { min, .. } => {
                        *min > 0
                            && edge_type_of(&self.catalog, &other.table)
                                .is_ok_and(|edge| table.table == format!("node:{}", edge.from_type))
                    }
                    _ => false,
                })
            })
    }

    pub(crate) fn sweep_plan(&self, index: usize, shards: usize) -> SweepPlan {
        let table = &self.tables[index];
        SweepPlan::new(
            &table.table,
            &table.checks,
            table.delta.replaces(),
            shards,
            self.repeat,
            self.report,
            self.needs_ids(index),
        )
    }

    /// The staged routes' door: every table's batches, already in memory,
    /// swept in order and judged at once against `db`. A plan that sweeps its
    /// endpoint checks reads the committed key sets after every table's sweep,
    /// as the `load csv` operator does before its partitions drain, and an
    /// endpoint whose node table the same write stages settles against that
    /// table's delta before the set. `Ok(())` admits the write under
    /// [`Report::First`]; under [`Report::Every`] the violations come back.
    pub(crate) async fn judge_batches(
        &self,
        db: &Omnigraph,
        batches: Vec<Vec<RecordBatch>>,
        admission: &mut LoadAdmission<'_>,
    ) -> Result<Vec<Violation>> {
        if batches.len() != self.tables.len() {
            return Err(OmniError::manifest_internal(format!(
                "Validate: {} deltas for {} judged tables",
                batches.len(),
                self.tables.len()
            )));
        }
        let system_columns = self.catalog.system_columns;
        let plans: Vec<SweepPlan> = (0..self.tables.len())
            .map(|index| self.sweep_plan(index, 1))
            .collect();
        let mut partials: Vec<SweepPartial> = Vec::with_capacity(plans.len());
        for (batches, plan) in batches.iter().zip(&plans) {
            let mut partial = SweepPartial::new(plan);
            let mut base: Position = 0;
            for batch in batches {
                partial.observe(plan, &self.catalog, batch, system_columns, base)?;
                base += batch.num_rows() as Position;
            }
            partials.push(partial);
        }
        let (_retained, _transient) = if plans.iter().any(SweepPlan::sweeps_endpoints) {
            let admitted =
                admit_swept_sets(self, &mut |bytes| admission.reserve_bytes(bytes)).await?;
            let (sets, retained, transient) = admitted.load(system_columns).await?;
            let present: Vec<HashSet<String>> = partials
                .iter_mut()
                .map(|partial| std::mem::take(&mut partial.ids))
                .collect();
            let deleted: Vec<HashSet<&str>> = self
                .tables
                .iter()
                .map(|table| table.deleted_ids.iter().map(String::as_str).collect())
                .collect();
            for (index, plan) in plans.iter().enumerate() {
                if !plan.sweeps_endpoints() {
                    continue;
                }
                let staged = StagedEndpoints::of(self, plan, &present, &deleted);
                let mut base: Position = 0;
                for batch in &batches[index] {
                    partials[index].probe_endpoints(
                        plan,
                        batch,
                        system_columns,
                        base,
                        &sets,
                        Some(&staged),
                    )?;
                    base += batch.num_rows() as Position;
                }
            }
            for (partial, ids) in partials.iter_mut().zip(present) {
                partial.ids = ids;
            }
            (retained, transient)
        } else {
            (Vec::new(), Vec::new())
        };
        let deltas = batches
            .into_iter()
            .zip(&plans)
            .zip(partials)
            .map(|((batches, plan), partial)| TableDelta {
                rows: batches.iter().map(RecordBatch::num_rows).sum(),
                batches,
                extracted: Extracted::prepare(plan, vec![partial]),
            })
            .collect();
        judge_write_swept(self, db, deltas, admission).await
    }
}

/// The same write's deltas of the swept endpoint node tables, by node type:
/// a staged route's probe settles an endpoint against them before the
/// committed key set, as the judged probe settles against its [`NodeDelta`].
pub(crate) struct StagedEndpoints<'d> {
    deltas: HashMap<&'d str, NodeDelta<'d>>,
}

impl<'d> StagedEndpoints<'d> {
    fn of(
        judge: &'d WriteJudge,
        plan: &'d SweepPlan,
        present: &'d [HashSet<String>],
        deleted: &'d [HashSet<&'d str>],
    ) -> Self {
        let deltas = plan
            .endpoints
            .iter()
            .filter_map(|swept| {
                let index = judge.table_index(&format!("node:{}", swept.node_type))?;
                Some((
                    swept.node_type.as_str(),
                    NodeDelta {
                        present: &present[index],
                        deleted: &deleted[index],
                    },
                ))
            })
            .collect();
        Self { deltas }
    }

    fn delta(&self, node_type: &str) -> Option<NodeDelta<'d>> {
        self.deltas.get(node_type).copied()
    }
}

/// One judged table's delta as the judgment reads it: the batches (empty
/// when the operator forwarded them), the rows, and the swept facts.
pub(crate) struct TableDelta {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) rows: usize,
    pub(crate) extracted: Extracted,
}

/// The ids a write removes from a node table, as an endpoint check reads
/// them: `present` is the same write's delta of the table, `deleted` the ids
/// it removes, so a delta edge's endpoint settles before the committed lookup.
#[derive(Clone, Copy)]
struct NodeDelta<'d> {
    present: &'d HashSet<String>,
    deleted: &'d HashSet<&'d str>,
}

impl NodeDelta<'_> {
    fn settles(&self, value: &str) -> bool {
        self.present.contains(value) || self.deleted.contains(value)
    }

    fn exists(&self, value: &str) -> Option<bool> {
        if self.deleted.contains(value) {
            Some(false)
        } else if self.present.contains(value) {
            Some(true)
        } else {
            None
        }
    }
}

/// Judge every table of `judge` over `deltas`, one per table in the judge's
/// order, the sweep already done by the caller: the `load csv` operator
/// sweeps each partition's batches as they arrive and merges the partials
/// ([`Extracted::prepare`]); [`WriteJudge::judge_batches`] sweeps in memory.
/// Every table's value check is judged first, then each table's remaining
/// checks in its node's order. A table's `removal` is the plan's word on the
/// ids a replacement removes: under `Scan` they are found before the checks
/// run, under `None` the plan carries no check that reads them, which the
/// committed table's row count must bear out. `db` is the handle whose live
/// branch a [`JudgedSide::Live`] judge counts `@card` on.
pub(crate) async fn judge_write_swept(
    judge: &WriteJudge,
    db: &Omnigraph,
    mut deltas: Vec<TableDelta>,
    admission: &mut LoadAdmission<'_>,
) -> Result<Vec<Violation>> {
    if deltas.len() != judge.tables.len() {
        return Err(OmniError::manifest_internal(format!(
            "Validate: {} deltas for {} judged tables",
            deltas.len(),
            judge.tables.len()
        )));
    }
    let catalog = judge.catalog.as_ref();
    let system_columns = catalog.system_columns;
    let base = judge.snapshot();
    let replaced = judge.replaced();
    let mut removed: Vec<Vec<String>> = Vec::with_capacity(judge.tables.len());
    for (table, delta) in judge.tables.iter().zip(&deltas) {
        if table.delta.replaces() && table.removal == ValidateRemoval::None {
            removal_omitted_soundly(&table.table, &table.checks, catalog, base).await?;
        }
        removed.push(
            if table.delta.replaces() && table.removal == ValidateRemoval::Scan {
                let _phase = admission
                    .phase
                    .as_mut()
                    .map(|admit| admit(LoadPhase::RemovedInput))
                    .transpose()?;
                overwrite_removed_ids(base, &table.table, &delta.batches, system_columns, |id| {
                    (admission.removed)(id)
                })
                .await?
            } else {
                Vec::new()
            },
        );
    }
    let gone: Vec<HashSet<&str>> = judge
        .tables
        .iter()
        .zip(&removed)
        .map(|(table, removed)| {
            table
                .deleted_ids
                .iter()
                .chain(removed.iter())
                .map(String::as_str)
                .collect()
        })
        .collect();
    let mut reuse = admission.reuse.as_mut().and_then(|admit| admit());
    let mut cached_base = reuse.as_ref().map(|reuse| {
        let mut snapshot = base.clone();
        snapshot.set_read_caches(reuse.snapshot_caches());
        snapshot
    });
    let integer_endpoints = judge.tables.iter().any(|table| {
        table.checks.iter().any(|spec| {
            matches!(&spec.check, ValidateCheck::EdgeRi { node_type, .. } if int_keyed(catalog, node_type))
        })
    });
    let mut verdict = Verdict::new(judge.report);
    for (index, table) in judge.tables.iter().enumerate() {
        if table
            .checks
            .iter()
            .any(|spec| matches!(spec.check, ValidateCheck::Value { .. }))
        {
            verdict.refuse_each(std::mem::take(&mut deltas[index].extracted.value))?;
        }
    }
    for (index, table) in judge.tables.iter().enumerate() {
        let table_key = table.table.as_str();
        let checks = table.checks.as_slice();
        let mut edge_ri_judged = false;
        let mut retained_judged: HashSet<&str> = HashSet::new();
        for (check_index, spec) in checks.iter().enumerate() {
            if matches!(spec.check, ValidateCheck::RetainedEdges { .. })
                || (integer_endpoints && matches!(spec.check, ValidateCheck::EdgeRi { .. }))
            {
                drop(cached_base.take());
                drop(reuse.take());
            }
            let committed = CommittedState {
                committed: Some(cached_base.as_ref().unwrap_or(base)),
                overwritten: replaced.clone(),
                live: judge.committed.live(db),
                opened: Default::default(),
            };
            match &spec.check {
                ValidateCheck::Value { .. } => {}
                ValidateCheck::Unique { columns, .. } => {
                    let (image, repeat) = deltas[index]
                        .extracted
                        .unique
                        .remove(&check_index)
                        .ok_or_else(|| missing_facts(spec))?;
                    verdict.refuse(repeat)?;
                    unique_coalesced_dups(table_key, columns, &image.entries, &mut verdict.sink())?;
                    verdict.settle()?;
                    if spec.consults.is_some() {
                        let probe = unique_probe_keys(&image.entries);
                        let holders = committed
                            .unique_holders(
                                table_key,
                                columns,
                                &probe,
                                system_columns,
                                access_path(spec)?,
                            )
                            .await?;
                        unique_committed_collisions(
                            table_key,
                            columns,
                            &image.entries,
                            &holders,
                            &deltas[index].extracted.ids,
                            &gone[index],
                            &mut verdict.sink(),
                        )?;
                        verdict.settle()?;
                    }
                }
                ValidateCheck::EdgeRi { .. } => {
                    if edge_ri_judged || deltas[index].rows == 0 {
                        continue;
                    }
                    edge_ri_judged = true;
                    if spec.probe == Some(EndpointProbe::Swept) {
                        verdict.refuse_each(std::mem::take(&mut deltas[index].extracted.orphan))?;
                        continue;
                    }
                    let edge_type = edge_type_of(catalog, table_key)?;
                    let access_of = |wanted: Endpoint| -> Result<AccessPath> {
                        let spec = checks
                            .iter()
                            .find(|spec| {
                                matches!(&spec.check, ValidateCheck::EdgeRi { endpoint, .. } if *endpoint == wanted)
                            })
                            .unwrap_or(spec);
                        access_path(spec)
                    };
                    let node_delta = |node_type: &str| {
                        judge
                            .table_index(&format!("node:{node_type}"))
                            .map(|node| NodeDelta {
                                present: &deltas[node].extracted.ids,
                                deleted: &gone[node],
                            })
                    };
                    let batches = deltas[index].batches.as_slice();
                    let from_keys = EndpointKeys::load(
                        &committed,
                        catalog,
                        &edge_type.from_type,
                        batches,
                        system_columns.src,
                        system_columns,
                        access_of(Endpoint::From)?,
                        node_delta(&edge_type.from_type),
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
                        node_delta(&edge_type.to_type),
                        admission,
                    )
                    .await?;
                    orphans(
                        table_key,
                        &edge_type.from_type,
                        &edge_type.to_type,
                        batches,
                        &from_keys,
                        &to_keys,
                        system_columns,
                        judge.report,
                        admission,
                        &mut verdict.sink(),
                    )?;
                    verdict.settle()?;
                }
                ValidateCheck::Cardinality { .. } => {
                    let edge_type = edge_type_of(catalog, table_key)?;
                    if edge_type.cardinality.is_default() {
                        continue;
                    }
                    if table.delta.replaces()
                        && edge_type.cardinality.min > 0
                        && spec.access != Some(ValidateAccess::Scan)
                    {
                        return Err(OmniError::manifest_internal(format!(
                            "Validate: the plan reads the replaced sources of {table_key} under {:?}, \
                             not a scan, so a source the overwrite empties would go uncounted",
                            spec.access
                        )));
                    }
                    let delta_by_id = std::mem::take(&mut deltas[index].extracted.card);
                    let plan = CardPlan {
                        changed_ids: delta_by_id.keys().cloned().collect(),
                        delta_by_id,
                        removed_ids: table.deleted_ids.clone(),
                    };
                    let source_table = format!("node:{}", edge_type.from_type);
                    let (written, deleted_sources) = match judge.table_index(&source_table) {
                        Some(source) => (
                            if edge_type.cardinality.min > 0 {
                                deltas[source].extracted.ids.clone()
                            } else {
                                HashSet::new()
                            },
                            gone[source].iter().map(|id| (*id).to_string()).collect(),
                        ),
                        None => (HashSet::new(), HashSet::new()),
                    };
                    super::evaluate_cardinality(
                        table_key,
                        edge_type,
                        &plan,
                        &written,
                        &deleted_sources,
                        &committed,
                        system_columns,
                        &mut verdict.sink(),
                    )
                    .await?;
                    verdict.settle()?;
                }
                ValidateCheck::SourceCardinality { edge_table, .. } => {
                    if judge.table_index(edge_table).is_some_and(|edge| {
                        judge.tables[edge]
                            .checks
                            .iter()
                            .any(|spec| matches!(spec.check, ValidateCheck::Cardinality { .. }))
                    }) {
                        continue;
                    }
                    let edge_type = edge_type_of(catalog, edge_table)?;
                    let deleted_sources: HashSet<String> =
                        gone[index].iter().map(|id| (*id).to_string()).collect();
                    super::evaluate_cardinality(
                        edge_table,
                        edge_type,
                        &CardPlan::default(),
                        &deltas[index].extracted.ids,
                        &deleted_sources,
                        &committed,
                        system_columns,
                        &mut verdict.sink(),
                    )
                    .await?;
                    verdict.settle()?;
                }
                ValidateCheck::RetainedEdges { edge_table, .. } => {
                    if !retained_judged.insert(edge_table) {
                        continue;
                    }
                    let edge_type = edge_type_of(catalog, edge_table)?;
                    let gone_here: Vec<String> =
                        gone[index].iter().map(|id| (*id).to_string()).collect();
                    let deleted_at = |wanted: Endpoint| -> &[String] {
                        let listed = checks.iter().any(|spec| {
                            matches!(&spec.check, ValidateCheck::RetainedEdges { edge_table: table, endpoint }
                                if table == edge_table && *endpoint == wanted)
                        });
                        if listed { gone_here.as_slice() } else { &[] }
                    };
                    let deleted_from = deleted_at(Endpoint::From);
                    let deleted_to = deleted_at(Endpoint::To);
                    let empty = HashSet::new();
                    let delta_edge_ids = match judge.table_index(edge_table) {
                        Some(edge) => &deltas[edge].extracted.ids,
                        None => &empty,
                    };
                    let removed_edges: HashSet<&String> = judge
                        .table_index(edge_table)
                        .map(|edge| judge.tables[edge].deleted_ids.iter().collect())
                        .unwrap_or_default();
                    let limit_one = judge.report == Report::First
                        && delta_edge_ids.is_empty()
                        && removed_edges.is_empty();
                    let retained = committed
                        .edges_referencing(
                            edge_table,
                            deleted_from,
                            deleted_to,
                            system_columns,
                            limit_one,
                            admission.retained,
                        )
                        .await?;
                    orphans_retained(
                        edge_table,
                        &edge_type.from_type,
                        &edge_type.to_type,
                        &retained,
                        delta_edge_ids,
                        &removed_edges,
                        deleted_from,
                        deleted_to,
                        system_columns,
                        &mut verdict.sink(),
                    )?;
                    verdict.settle()?;
                }
            }
        }
    }
    Ok(verdict.finish())
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
    /// partition inserts into as it sweeps, read under
    /// [`RepeatRule::AcrossInput`] only.
    unique_holders: Vec<Arc<SharedFirstHolders>>,
    repeat: RepeatRule,
    report: Report,
    input_only: bool,
    /// The swept endpoint checks, `From` before `To`, so a row whose src and
    /// dst both miss reports its src as [`orphans`] does.
    endpoints: Vec<SweptEndpoint>,
}

struct SweptEndpoint {
    endpoint: Endpoint,
    node_type: String,
}

impl SweepPlan {
    /// `shards` is the partitions that sweep at once; each uniqueness check's
    /// shared holders split into that many locked maps, so the partitions
    /// rarely wait on one another. `keeps_ids` asks for every id of the delta
    /// beyond what the table's own checks read ([`WriteJudge::needs_ids`]).
    pub(crate) fn new(
        table: &str,
        checks: &[ValidateSpec],
        overwrite: bool,
        shards: usize,
        repeat: RepeatRule,
        report: Report,
        keeps_ids: bool,
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
            needs_ids: keeps_ids
                || checks.iter().any(|spec| match spec.check {
                    ValidateCheck::Unique { .. } => spec.consults.is_some(),
                    ValidateCheck::SourceCardinality { .. } => true,
                    _ => false,
                }),
            needs_card: checks
                .iter()
                .any(|spec| matches!(spec.check, ValidateCheck::Cardinality { .. })),
            unique_holders: unique
                .iter()
                .map(|_| Arc::new(SharedFirstHolders::new(shards)))
                .collect(),
            unique,
            repeat,
            report,
            input_only: overwrite,
            endpoints,
        }
    }

    /// Whether the partitions probe the endpoint checks as they sweep.
    pub(crate) fn sweeps_endpoints(&self) -> bool {
        !self.endpoints.is_empty()
    }

    /// Whether a uniqueness check's holders are sized and shared.
    pub(crate) fn has_unique_holders(&self) -> bool {
        self.repeat == RepeatRule::AcrossInput && !self.unique_holders.is_empty()
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

/// Admit the swept endpoint key sets of the judge's tables: the input,
/// retained and build scratch bytes of each distinct endpoint table's row
/// count.
pub(crate) async fn admit_swept_sets(
    judge: &WriteJudge,
    admit: &mut ByteAdmission<'_>,
) -> Result<AdmittedSets> {
    let committed = CommittedState {
        committed: Some(judge.snapshot()),
        overwritten: judge.replaced(),
        live: None,
        opened: Default::default(),
    };
    let mut admitted = AdmittedSets {
        tables: Vec::new(),
        retained: Vec::new(),
        transient: Vec::new(),
    };
    for spec in judge.tables.iter().flat_map(|table| &table.checks) {
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
                scan_int_ids(&ds, system_columns.id, &mut keys, bound, &|_| true).await?;
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
    /// The endpoint misses among the partition's rows, in row order, when the
    /// plan sweeps the endpoint checks: the earliest alone under [`Report::First`].
    orphan: Vec<(Position, Violation)>,
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
                .map(|holders| {
                    UniquePartial::new(
                        plan.input_only,
                        match plan.repeat {
                            RepeatRule::AcrossInput => Repeats::Shared(Arc::clone(holders)),
                            RepeatRule::ByFinalKey => Repeats::WithinBatch(None),
                        },
                    )
                })
                .collect(),
            card: HashMap::new(),
            orphan: Vec::new(),
            parsed: Vec::new(),
        }
    }

    /// Probe `batch`'s swept endpoints against `sets`, an endpoint `staged`
    /// settles first: row order, src before dst within a row, as [`orphans`]
    /// reports it over the whole delta; the earliest miss under
    /// [`Report::First`], every miss under [`Report::Every`]. `base` is the
    /// batch's first row.
    pub(crate) fn probe_endpoints(
        &mut self,
        plan: &SweepPlan,
        batch: &RecordBatch,
        system_columns: SystemColumns,
        base: Position,
        sets: &SweptSets,
        staged: Option<&StagedEndpoints<'_>>,
    ) -> Result<()> {
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
            let delta = staged.and_then(|staged| staged.delta(&swept.node_type));
            let rows = int_misses(keys, values, &mut self.parsed, plan.report, delta);
            if rows.is_empty() {
                continue;
            }
            let ids = string_col(batch, system_columns.id)?;
            for row in rows {
                self.orphan.push((
                    base.saturating_add(row as Position),
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
        keep_orphans(&mut self.orphan, plan.report);
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
    /// Per violated value constraint in declaration order, its earliest
    /// violating row anywhere in the delta; the first is the one a load or a
    /// mutation refuses on.
    value: Vec<Violation>,
    /// Every non-null id of the delta, when a consulted uniqueness or a
    /// source cardinality check reads it.
    ids: HashSet<String>,
    /// Per uniqueness check (by list index): its image and the first key
    /// repeated anywhere in the input.
    unique: HashMap<usize, (UniqueImage, Option<Violation>)>,
    /// Last-wins src per edge id, when a cardinality check reads it.
    card: HashMap<String, String>,
    /// The endpoint misses of the delta in row order, when the plan swept the
    /// endpoint checks: the earliest alone under [`Report::First`].
    orphan: Vec<Violation>,
}

impl Extracted {
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
        let mut repeats: Vec<Option<(Position, Violation)>> =
            plan.unique.iter().map(|_| None).collect();
        let mut orphan: Vec<(Position, Violation)> = Vec::new();
        for partial in partials {
            orphan.extend(partial.orphan);
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
                let (keys, repeat) = unique_partial.into_parts();
                finals[index].push(keys);
                if let Some((position, violation)) = repeat
                    && repeats[index].as_ref().is_none_or(|(at, _)| position < *at)
                {
                    repeats[index] = Some((position, violation));
                }
            }
        }
        let unique = plan
            .unique
            .iter()
            .zip(finals)
            .zip(&plan.unique_holders)
            .zip(repeats)
            .map(|((((check, _), finals), holders), within_batch)| {
                let repeat = match plan.repeat {
                    RepeatRule::AcrossInput => holders.first_repeat(),
                    RepeatRule::ByFinalKey => within_batch,
                }
                .map(|(_, violation)| violation);
                let image = merge_unique_image(finals);
                (*check, (image, repeat))
            })
            .collect();
        keep_orphans(&mut orphan, plan.report);
        Self {
            value: value
                .into_iter()
                .flatten()
                .map(|(_, violation)| violation)
                .collect(),
            ids,
            unique,
            card: card.into_iter().map(|(id, (_, src))| (id, src)).collect(),
            orphan: orphan.into_iter().map(|(_, violation)| violation).collect(),
        }
    }
}

/// Row order, src before dst within a row (the probes push `From` before
/// `To`); the earliest alone under [`Report::First`].
fn keep_orphans(orphans: &mut Vec<(Position, Violation)>, report: Report) {
    orphans.sort_by_key(|(position, _)| *position);
    if report == Report::First {
        orphans.truncate(1);
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

/// What the finalizers report, under the judge's [`Report`]: the first
/// violation of a check, refused once the check is settled; or every
/// violation, collected.
struct Verdict {
    report: Report,
    pending: Option<Violation>,
    every: Vec<Violation>,
}

impl Verdict {
    fn new(report: Report) -> Self {
        Self {
            report,
            pending: None,
            every: Vec::new(),
        }
    }

    fn sink(&mut self) -> impl FnMut(Violation) -> Result<()> + '_ {
        move |violation| {
            match self.report {
                Report::First => {
                    self.pending.get_or_insert(violation);
                }
                Report::Every => self.every.push(violation),
            }
            Ok(())
        }
    }

    fn settle(&mut self) -> Result<()> {
        let pending = self.pending.take();
        self.refuse(pending)
    }

    fn refuse(&mut self, violation: Option<Violation>) -> Result<()> {
        match (self.report, violation) {
            (_, None) => Ok(()),
            (Report::First, Some(violation)) => Err(violation.into_omni_error()),
            (Report::Every, Some(violation)) => {
                self.every.push(violation);
                Ok(())
            }
        }
    }

    fn refuse_each(&mut self, violations: Vec<Violation>) -> Result<()> {
        for violation in violations {
            self.refuse(Some(violation))?;
        }
        Ok(())
    }

    fn finish(self) -> Vec<Violation> {
        self.every
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

/// The ids an endpoint check settles against: the committed ids (an integer
/// key set under one `I32`/`I64`/`U32` `@key`, the id strings otherwise) and
/// the same write's delta of the table, consulted before the committed lookup.
enum EndpointKeys<'d> {
    Int {
        keys: IntKeySet,
        _memory: Box<dyn Send>,
        delta: Option<NodeDelta<'d>>,
    },
    Text {
        ids: HashSet<String>,
        delta: Option<NodeDelta<'d>>,
    },
}

impl<'d> EndpointKeys<'d> {
    #[allow(clippy::too_many_arguments)]
    async fn load(
        committed: &CommittedState<'_>,
        catalog: &Catalog,
        node_type: &str,
        batches: &[RecordBatch],
        column: &str,
        system_columns: SystemColumns,
        access: AccessPath,
        delta: Option<NodeDelta<'d>>,
        admission: &mut LoadAdmission<'_>,
    ) -> Result<Self> {
        let table_key = format!("node:{node_type}");
        let _wanted_memory = admission.reserve_bytes(endpoint_value_bytes(batches, column)?)?;
        let mut wanted = endpoint_values(batches, column)?;
        if let Some(delta) = delta {
            wanted.retain(|value| !delta.settles(value));
        }
        if int_keyed(catalog, node_type) {
            let (keys, memory) = committed
                .existing_int_keys(&table_key, &wanted, system_columns, access, &mut |bytes| {
                    admission.reserve_bytes(bytes)
                })
                .await?;
            return Ok(Self::Int {
                keys,
                _memory: memory,
                delta,
            });
        }
        let ids = committed
            .existing_ids(&table_key, &wanted, system_columns, access)
            .await?;
        Ok(Self::Text { ids, delta })
    }

    /// The rows of `column` naming no id of the table (a null or non-canonical
    /// spelling misses), the first under `Report::First` and every one under
    /// `Report::Every`; integer keys parse into `parsed` for the whole column first.
    fn misses(&self, column: &StringArray, parsed: &mut Vec<i64>, report: Report) -> Vec<usize> {
        let mut rows = Vec::new();
        match self {
            Self::Int { keys, delta, .. } => {
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
                for (row, key) in parsed.iter().enumerate() {
                    let settled = delta.and_then(|delta| delta.exists(column.value(row)));
                    if !settled.unwrap_or_else(|| keys.contains(*key)) {
                        rows.push(row);
                        if report == Report::First {
                            return rows;
                        }
                    }
                }
                if let Some(row) = unparsed {
                    rows.push(row);
                }
            }
            Self::Text { ids, delta } => {
                for row in 0..column.len() {
                    let exists = !column.is_null(row) && {
                        let value = column.value(row);
                        delta
                            .and_then(|delta| delta.exists(value))
                            .unwrap_or_else(|| ids.contains(value))
                    };
                    if !exists {
                        rows.push(row);
                        if report == Report::First {
                            return rows;
                        }
                    }
                }
            }
        }
        rows
    }
}

/// The rows of `column` naming no integer key of `delta`, the same write's
/// node delta asked first, or of the committed set (a null or a non-canonical
/// spelling misses), the first alone under [`Report::First`], parsed as one column.
fn int_misses(
    keys: &IntKeySet,
    column: &StringArray,
    parsed: &mut Vec<i64>,
    report: Report,
    delta: Option<NodeDelta<'_>>,
) -> Vec<usize> {
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
    let mut rows = Vec::new();
    for (row, key) in parsed.iter().enumerate() {
        let settled = delta.and_then(|delta| delta.exists(column.value(row)));
        if !settled.unwrap_or_else(|| keys.contains(*key)) {
            rows.push(row);
            if report == Report::First {
                return rows;
            }
        }
    }
    rows.extend(unparsed);
    rows
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

/// The delta edges, in row order and src before dst, whose endpoint is
/// absent from its node table: the first one under `Report::First`, every
/// one under `Report::Every`.
#[allow(clippy::too_many_arguments)]
fn orphans<F>(
    edge_table: &str,
    from_type: &str,
    to_type: &str,
    batches: &[RecordBatch],
    from_keys: &EndpointKeys<'_>,
    to_keys: &EndpointKeys<'_>,
    system_columns: SystemColumns,
    report: Report,
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
        let src_misses = from_keys.misses(srcs, &mut parsed, report);
        let dst_misses = to_keys.misses(dsts, &mut parsed, report);
        let mut src_misses = src_misses.into_iter().peekable();
        let mut dst_misses = dst_misses.into_iter().peekable();
        loop {
            let (row, column, values, node_type) = match (src_misses.peek(), dst_misses.peek()) {
                (None, None) => break,
                (Some(src), Some(dst)) if dst < src => {
                    (dst_misses.next(), system_columns.dst, dsts, to_type)
                }
                (Some(_), _) => (src_misses.next(), system_columns.src, srcs, from_type),
                (None, Some(_)) => (dst_misses.next(), system_columns.dst, dsts, to_type),
            };
            let Some(row) = row else {
                break;
            };
            sink(orphan_violation(
                edge_table,
                ids.value(row),
                column,
                values.value(row),
                node_type,
            ))?;
            if report == Report::First {
                return Ok(());
            }
        }
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
    use crate::loader::LoadMode;

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

    /// A one-table judge over the load's checks against `db`'s snapshot, the
    /// `load csv` shape: the base as the committed side, first violation,
    /// repeats across the input.
    async fn write_judge(
        db: &Session,
        table: &str,
        checks: Vec<ValidateSpec>,
        mode: LoadStmtMode,
        removal: ValidateRemoval,
    ) -> WriteJudge {
        WriteJudge {
            tables: vec![JudgedTable {
                table: table.to_string(),
                checks,
                removal,
                delta: match mode {
                    LoadStmtMode::Overwrite => DeltaClass::Replace,
                    LoadStmtMode::Append => DeltaClass::Insert,
                    LoadStmtMode::Merge => DeltaClass::Upsert,
                },
                deleted_ids: Vec::new(),
            }],
            committed: JudgedSide::Base(db.snapshot().await),
            write_budget: WriteBudget::from_settings(db.settings()),
            catalog: db.catalog(),
            report: Report::First,
            repeat: RepeatRule::AcrossInput,
        }
    }

    fn no_admission<'a>(
        removed: &'a mut (dyn FnMut(&str) -> Result<()> + Send),
        retained: &'a mut (dyn FnMut(&RecordBatch) -> Result<()> + Send),
    ) -> LoadAdmission<'a> {
        LoadAdmission {
            phase: None,
            bytes: None,
            reuse: None,
            removed,
            retained,
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
        let table = target.table_key();
        let checks = checks(&catalog, target, mode, access);
        write_judge(db, &table, checks, mode, ValidateRemoval::Scan)
            .await
            .judge_batches(
                db,
                vec![batches.to_vec()],
                &mut no_admission(&mut |_| Ok(()), &mut |_| Ok(())),
            )
            .await
            .map(|_| ())
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
        let judge = write_judge(
            &db,
            "node:Person",
            checks,
            LoadStmtMode::Overwrite,
            ValidateRemoval::Scan,
        )
        .await;
        let mut admitted = Vec::new();
        let refused = judge
            .judge_batches(
                &db,
                vec![vec![person_batch(
                    sc,
                    &[("Alice", "alice@x"), ("Bob", "bob@x")],
                )]],
                &mut no_admission(
                    &mut |id| {
                        admitted.push(id.to_string());
                        Ok(())
                    },
                    &mut |_| Ok(()),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(
            refused.to_string(),
            format!("{} 'Carol' not found in Person", sc.dst)
        );
        assert_eq!(admitted, ["Carol"]);

        judge
            .judge_batches(
                &db,
                vec![vec![person_batch(
                    sc,
                    &[("Alice", "alice@x"), ("Bob", "bob@x"), ("Carol", "carol@x")],
                )]],
                &mut no_admission(&mut |_| Ok(()), &mut |_| Ok(())),
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
        let checks = checks(
            &catalog,
            LoadTarget::Node("Person".into()),
            LoadStmtMode::Overwrite,
            ValidateAccess::Scan,
        );
        let batches = vec![person_batch(
            sc,
            &[("Alice", "alice@x"), ("Bob", "bob@x"), ("Carol", "carol@x")],
        )];
        let listed = write_judge(
            &db,
            "node:Person",
            checks.clone(),
            LoadStmtMode::Overwrite,
            ValidateRemoval::None,
        )
        .await
        .judge_batches(
            &db,
            vec![batches.clone()],
            &mut no_admission(&mut |_| Ok(()), &mut |_| Ok(())),
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
        let populated = write_judge(
            &db,
            "node:Person",
            pruned,
            LoadStmtMode::Overwrite,
            ValidateRemoval::None,
        )
        .await
        .judge_batches(
            &db,
            vec![batches],
            &mut no_admission(&mut |_| Ok(()), &mut |_| Ok(())),
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
