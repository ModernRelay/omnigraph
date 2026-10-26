//! A load's validation over every partition of its input, every check of the
//! plan extracted in one pass per batch. Judged per row (the planner's
//! `validate_streams`), it sends each batch on as it passes and the judgment
//! after the drain is the stream's last word; otherwise it is a pipeline
//! breaker that holds the whole delta, restores source order by slice, and
//! streams the held batches on only when the delta is admitted.

use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, LargeStringArray, RecordBatch, StringArray, StringViewArray};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use omnigraph_compiler::catalog::Catalog;
use omnigraph_planner::{ValidateRemoval, ValidateSpec};

use futures::StreamExt;
use tokio::sync::watch;

use super::csv_source::{CsvSourceExec, FailedSlice, slice_of};
use super::external;
use super::memory::WorkMemory;
use super::shape::{BatchSender, Breaker, Streaming, work};
use crate::db::{Omnigraph, Snapshot};
use crate::error::OmniError;
use crate::loader::LoadMode;
use crate::loader::read_reuse::LoadReadReuse;
use crate::storage_layer::{DeletedIdBudget, WriteBudget};
use crate::validate::load::{
    AdmittedSets, Extracted, LoadAdmission, LoadPhase, SweepPartial, SweepPlan, SweptSets,
    admit_swept_sets, int_keyed, judge_load_swept, parse_scratch_bytes, position,
};
use omnigraph_compiler::SystemColumns;
use omnigraph_compiler::ir::ValidateCheck;

/// The swept endpoint checks' key sets as the partitions receive them:
/// `None` until the loader publishes, the sets once it has.
type PublishedSets = watch::Receiver<Option<Arc<SweptSets>>>;

/// The `validated_rows` metric: the delta rows the operator judged.
pub(crate) const VALIDATED_ROWS: &str = "validated_rows";

/// What one validation judges against: the plan's checks, the statement's
/// mode, and the committed image the transaction pins.
pub(crate) struct LoadJudge {
    pub(crate) db: Arc<Omnigraph>,
    pub(crate) table: String,
    pub(crate) checks: Vec<ValidateSpec>,
    pub(crate) removal: ValidateRemoval,
    pub(crate) mode: LoadMode,
    pub(crate) write_budget: WriteBudget,
    pub(crate) snapshot: Snapshot,
    pub(crate) catalog: Arc<Catalog>,
}

/// The shape the plan's cut declares for the validation, the same fact the
/// planner's `validate_streams` reads off the node.
enum ValidateShape {
    Streaming(Streaming),
    Breaker(Breaker),
}

impl ValidateShape {
    fn new(schema: arrow_schema::SchemaRef, streams: bool) -> Self {
        if streams {
            Self::Streaming(Streaming::new(schema))
        } else {
            Self::Breaker(Breaker::new(schema))
        }
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        match self {
            Self::Streaming(shape) => shape.properties(),
            Self::Breaker(shape) => shape.properties(),
        }
    }
}

/// The partitions' way of handing batches on: the shared sender of the
/// streaming shape, each batch sent as it passes; nothing for the breaker,
/// which holds them.
type Forward = Option<Arc<tokio::sync::Mutex<BatchSender>>>;

pub(crate) struct ValidateExec {
    input: Arc<dyn ExecutionPlan>,
    judge: Arc<LoadJudge>,
    shape: ValidateShape,
    metrics: ExecutionPlanMetricsSet,
}

impl ValidateExec {
    /// `streams` is the planner's `validate_streams` for the node's checks.
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, judge: LoadJudge, streams: bool) -> Self {
        let schema = input.schema();
        Self {
            input,
            judge: Arc::new(judge),
            shape: ValidateShape::new(schema, streams),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for ValidateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ValidateExec")
            .field("table", &self.judge.table)
            .field("checks", &self.judge.checks.len())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ValidateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let checks: Vec<&str> = self
            .judge
            .checks
            .iter()
            .map(|spec| spec.check.name())
            .collect();
        write!(
            f,
            "ValidateExec: {} [{}]",
            self.judge.table,
            checks.join(", ")
        )
    }
}

impl ExecutionPlan for ValidateExec {
    fn name(&self) -> &str {
        "ValidateExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "ValidateExec has one child");
        let input = children.remove(0);
        let streams = matches!(self.shape, ValidateShape::Streaming(_));
        Ok(Arc::new(Self {
            shape: ValidateShape::new(input.schema(), streams),
            input,
            judge: Arc::clone(&self.judge),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ValidateExec has one partition");
        let partitions = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        let source = Arc::clone(&self.input);
        let schema = self.input.schema();
        let memory = work(Arc::clone(&ctx), "ValidateExec", &self.metrics)?;
        memory.metric("input_rows", 0);
        memory.metric(VALIDATED_ROWS, 0);
        let judge = Arc::clone(&self.judge);
        let plan = Arc::new(SweepPlan::new(
            &judge.table,
            &judge.checks,
            judge.mode == LoadMode::Overwrite,
            partitions,
        ));
        match &self.shape {
            ValidateShape::Breaker(breaker) => Ok(breaker.held(
                memory,
                &self.metrics,
                move |memory| async move {
                    let drained = drain(
                        partitions, ctx, &memory, &source, &schema, &plan, &judge, None,
                    )
                    .await?;
                    let batches = drained.batches;
                    judge_after_drain(
                        &memory,
                        &judge,
                        &plan,
                        &batches,
                        drained.rows,
                        drained.partials,
                    )
                    .await?;
                    drop(drained.facts);
                    Ok(batches)
                },
                move |batches, memory, sender| async move {
                    for batch in batches {
                        sender.send(batch, Arc::clone(&memory)).await?;
                    }
                    Ok(())
                },
            )),
            ValidateShape::Streaming(streaming) => Ok(streaming.drive(
                memory,
                &self.metrics,
                move |memory, sender| async move {
                    let forward = Some(Arc::new(tokio::sync::Mutex::new(sender)));
                    let drained = drain(
                        partitions, ctx, &memory, &source, &schema, &plan, &judge, forward,
                    )
                    .await?;
                    judge_after_drain(&memory, &judge, &plan, &[], drained.rows, drained.partials)
                        .await?;
                    drop(drained.facts);
                    Ok(())
                },
            )),
        }
    }
}

/// What the drain left for the judgment: the held batches in source order
/// (none when the partitions forwarded them), the rows that passed, each
/// partition's facts and the memory that holds them.
struct Drained {
    batches: Vec<RecordBatch>,
    rows: usize,
    partials: Vec<SweepPartial>,
    facts: Vec<WorkMemory>,
}

/// The swept sets admitted, then the source's `partitions` executed and every
/// one drained beside the set load; the loaded sets' transient guards are
/// released once the drain ends.
#[allow(clippy::too_many_arguments)]
async fn drain(
    partitions: usize,
    ctx: Arc<TaskContext>,
    memory: &Arc<WorkMemory>,
    source: &Arc<dyn ExecutionPlan>,
    schema: &arrow_schema::SchemaRef,
    plan: &Arc<SweepPlan>,
    judge: &Arc<LoadJudge>,
    forward: Forward,
) -> DfResult<Drained> {
    let admitted = admit_sets(memory, judge, plan).await?;
    let inputs = (0..partitions)
        .map(|partition| source.execute(partition, Arc::clone(&ctx)))
        .collect::<DfResult<Vec<_>>>()?;
    let (publish, published) = watch::channel(None::<Arc<SweptSets>>);
    let (drained, set_guards) = futures::join!(
        drain_and_sweep(
            inputs, memory, source, schema, plan, judge, published, forward
        ),
        load_sets(Arc::clone(memory), Arc::clone(judge), admitted, publish)
    );
    let (_retained_sets, transient) = set_guards?;
    let drained = drained?;
    drop(transient);
    Ok(drained)
}

/// The checks judged over what the sweep extracted and, for the breaker,
/// the held batches; the unique holders freed after the verdict.
async fn judge_after_drain(
    memory: &Arc<WorkMemory>,
    judge: &Arc<LoadJudge>,
    plan: &Arc<SweepPlan>,
    batches: &[RecordBatch],
    rows: usize,
    partials: Vec<SweepPartial>,
) -> DfResult<()> {
    let extracted = Extracted::prepare(plan, partials);
    {
        let charged = memory.child("load removed and retained rows")?;
        let mut admit_phase =
            |LoadPhase::RemovedInput: LoadPhase| -> crate::error::Result<Box<dyn Send>> {
                let phase_memory = memory
                    .child("load removed input")
                    .map_err(|error| memory.error(error))?;
                phase_memory
                    .entries::<&str>(rows)
                    .map_err(|error| memory.error(error))?;
                Ok(Box::new(phase_memory))
            };
        let mut admit_bytes = |bytes: usize| -> crate::error::Result<Box<dyn Send>> {
            let allocation = memory
                .child("load integer endpoint keys")
                .map_err(|error| memory.error(error))?;
            allocation
                .grow(bytes)
                .map_err(|error| memory.error(error))?;
            Ok(Box::new(allocation))
        };
        let mut admit_reuse = || {
            memory
                .admit_read_reuse()
                .and_then(|admitted| LoadReadReuse::new(&judge.db, &judge.snapshot, admitted))
        };
        let mut deleted = DeletedIdBudget::new(judge.write_budget);
        let mut admit_removed = |id: &str| {
            deleted.retain(id)?;
            let reserve = || {
                charged.entries::<String>(1)?;
                charged.entries::<Expr>(2)?;
                charged.entries::<&String>(2)?;
                charged.string(id.len().saturating_mul(3))
            };
            reserve().map_err(|error| charged.error(error))
        };
        let mut admit_retained = |batch: &RecordBatch| {
            let reserve = || {
                charged.hold(batch)?;
                charged.entries::<(String, String, String)>(1)?;
                charged.grow(batch.get_array_memory_size().saturating_mul(2))
            };
            reserve().map_err(|error| charged.error(error))
        };
        judge_load_swept(
            &judge.table,
            &judge.checks,
            judge.removal,
            &judge.catalog,
            &judge.snapshot,
            judge.mode,
            batches,
            rows,
            extracted,
            &mut LoadAdmission {
                phase: Some(&mut admit_phase),
                bytes: Some(&mut admit_bytes),
                reuse: Some(&mut admit_reuse),
                removed: &mut admit_removed,
                retained: &mut admit_retained,
            },
        )
        .await
        .map_err(external)?;
    }
    release_holders(memory, plan).await?;
    memory.metric(VALIDATED_ROWS, rows);
    Ok(())
}

/// Every input partition drained and swept at once ([`sweep_partition`]), the
/// held batches sorted back into slice order under `schema`; the refusal is a
/// sliceless failure first, then the best [`FailedSlice::rank`], any partition.
#[allow(clippy::too_many_arguments)]
async fn drain_and_sweep(
    inputs: Vec<SendableRecordBatchStream>,
    memory: &Arc<WorkMemory>,
    source: &Arc<dyn ExecutionPlan>,
    schema: &arrow_schema::SchemaRef,
    plan: &Arc<SweepPlan>,
    judge: &Arc<LoadJudge>,
    published: PublishedSets,
    forward: Forward,
) -> DfResult<Drained> {
    let drained = futures::future::join_all(inputs.into_iter().map(|input| {
        let plan = Arc::clone(plan);
        let judge = Arc::clone(judge);
        let source = Arc::clone(source);
        let published = published.clone();
        let forward = forward.clone();
        memory.blocking(move |memory| {
            sweep_partition(input, memory, source, plan, judge, published, forward)
        })
    }))
    .await;
    let mut batches = Vec::new();
    let mut rows = 0;
    let mut partials = Vec::new();
    let mut facts = Vec::new();
    let mut failures = Vec::new();
    for (partition, result) in drained.into_iter().enumerate() {
        match result {
            Ok(swept) => {
                batches.extend(swept.held);
                rows += swept.rows;
                partials.push(swept.partial);
                facts.push(swept.facts);
            }
            Err(error) => failures.push((partition, error)),
        }
    }
    if !failures.is_empty() {
        let failed = source
            .downcast_ref::<CsvSourceExec>()
            .map(CsvSourceExec::failed_slices)
            .unwrap_or_default();
        let rank_of_partition = |partition: usize| {
            failed
                .iter()
                .filter(|failed| failed.partition == partition)
                .map(FailedSlice::rank)
                .min()
        };
        let winner = failures
            .iter()
            .position(|(partition, _)| rank_of_partition(*partition).is_none())
            .or_else(|| {
                failures
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, (partition, _))| rank_of_partition(*partition))
                    .map(|(index, _)| index)
            })
            .unwrap_or(0);
        return Err(failures.swap_remove(winner).1);
    }
    batches.sort_by_key(slice_of);
    let batches = batches
        .into_iter()
        .map(|batch| {
            RecordBatch::try_new(Arc::clone(schema), batch.columns().to_vec())
                .map_err(|error| external(OmniError::arrow_internal(error)))
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(Drained {
        batches,
        rows,
        partials,
        facts,
    })
}

/// The swept endpoint checks' key sets admitted on a worker before the source
/// partitions are executed, so every set's charge precedes every charge of the
/// parse and a refused set precedes them all.
async fn admit_sets(
    memory: &Arc<WorkMemory>,
    judge: &Arc<LoadJudge>,
    plan: &SweepPlan,
) -> DfResult<Option<AdmittedSets>> {
    if !plan.sweeps_endpoints() {
        return Ok(None);
    }
    let judge = Arc::clone(judge);
    memory
        .blocking(move |memory| async move {
            let mut admit = |bytes: usize| -> crate::error::Result<Box<dyn Send>> {
                let allocation = memory
                    .child("load integer endpoint keys")
                    .map_err(|error| memory.error(error))?;
                allocation
                    .grow(bytes)
                    .map_err(|error| memory.error(error))?;
                Ok(Box::new(allocation))
            };
            admit_swept_sets(
                &judge.table,
                &judge.checks,
                &judge.snapshot,
                judge.mode == LoadMode::Overwrite,
                &mut admit,
            )
            .await
            .map(Some)
            .map_err(external)
        })
        .await
}

/// The admitted sets read on a worker beside the drain and published to every
/// partition; the retained guards hold the sets until the judgment, the transient
/// ones the caller releases after the drain. A failed read drops the sender.
async fn load_sets(
    memory: Arc<WorkMemory>,
    judge: Arc<LoadJudge>,
    admitted: Option<AdmittedSets>,
    publish: watch::Sender<Option<Arc<SweptSets>>>,
) -> DfResult<(Vec<Box<dyn Send>>, Vec<Box<dyn Send>>)> {
    let Some(admitted) = admitted else {
        return Ok((Vec::new(), Vec::new()));
    };
    let system_columns = judge.catalog.system_columns;
    let (sets, retained, transient) = memory
        .blocking(move |_| async move { admitted.load(system_columns).await.map_err(external) })
        .await?;
    publish.send_replace(Some(Arc::new(sets)));
    Ok((retained, transient))
}

/// One partition's drain: what it held (nothing when it forwarded), the rows
/// it saw, its facts and the memory that holds them.
struct Swept {
    held: Vec<RecordBatch>,
    rows: usize,
    partial: SweepPartial,
    facts: WorkMemory,
}

/// One partition's input swept as it arrives (facts at its position, probed
/// once the swept sets land), then held under `memory` or sent on through
/// `forward` once probed, a batch from before the sets waiting in order.
async fn sweep_partition(
    mut input: SendableRecordBatchStream,
    memory: Arc<WorkMemory>,
    source: Arc<dyn ExecutionPlan>,
    plan: Arc<SweepPlan>,
    judge: Arc<LoadJudge>,
    mut published: PublishedSets,
    forward: Forward,
) -> DfResult<Swept> {
    let facts = memory.child("load validation facts")?;
    let waiting = memory.child("load batches before the sets")?;
    let mut partial = SweepPartial::new(&plan);
    let mut held = Vec::new();
    let mut rows = 0;
    let mut slice = usize::MAX;
    let mut offset = 0_u64;
    let system_columns = judge.catalog.system_columns;
    let mut sets: Option<Arc<SweptSets>> = None;
    let mut unprobed = Vec::new();
    let mut scratch_rows = 0usize;
    let mut sized = false;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        memory.metric("input_rows", batch.num_rows());
        rows += batch.num_rows();
        if forward.is_none() {
            memory.hold(&batch)?;
            memory.entries::<RecordBatch>(1)?;
        }
        let tagged = slice_of(&batch);
        if tagged != slice {
            slice = tagged;
            offset = 0;
        }
        let base = position(slice, offset);
        admit_validation(&facts, &judge, std::slice::from_ref(&batch))?;
        if !sized {
            sized = size_unique_holders(&facts, &plan, source.as_ref())?;
        }
        partial
            .observe(&plan, &judge.catalog, &batch, system_columns, base)
            .map_err(external)?;
        offset += batch.num_rows() as u64;
        if plan.sweeps_endpoints() {
            if sets.is_none() {
                sets = published.borrow().clone();
            }
            match &sets {
                Some(sets) => {
                    if !unprobed.is_empty() {
                        probe_held(
                            &facts,
                            &mut partial,
                            &plan,
                            &held,
                            &mut unprobed,
                            system_columns,
                            sets,
                            &mut scratch_rows,
                        )?;
                        send_held(&forward, &memory, &waiting, &mut held).await?;
                    }
                    probe_batch(
                        &facts,
                        &mut partial,
                        &plan,
                        &batch,
                        system_columns,
                        base,
                        sets,
                        &mut scratch_rows,
                    )?;
                }
                None => {
                    if forward.is_some() {
                        waiting.hold(&batch)?;
                    }
                    unprobed.push((held.len(), base));
                    held.push(batch);
                    continue;
                }
            }
        }
        match &forward {
            Some(sender) => sender.lock().await.send(batch, Arc::clone(&memory)).await?,
            None => held.push(batch),
        }
    }
    if !unprobed.is_empty() {
        let sets = match sets {
            Some(sets) => sets,
            None => loop {
                if let Some(sets) = published.borrow_and_update().clone() {
                    break sets;
                }
                published.changed().await.map_err(|_| {
                    external(OmniError::manifest_internal(
                        "the swept endpoint key sets were not published",
                    ))
                })?;
            },
        };
        probe_held(
            &facts,
            &mut partial,
            &plan,
            &held,
            &mut unprobed,
            system_columns,
            &sets,
            &mut scratch_rows,
        )?;
        send_held(&forward, &memory, &waiting, &mut held).await?;
    }
    Ok(Swept {
        held,
        rows,
        partial,
        facts,
    })
}

/// Probe the held batches that arrived before the sets, in order.
#[allow(clippy::too_many_arguments)]
fn probe_held(
    facts: &WorkMemory,
    partial: &mut SweepPartial,
    plan: &SweepPlan,
    held: &[RecordBatch],
    unprobed: &mut Vec<(usize, u64)>,
    system_columns: SystemColumns,
    sets: &SweptSets,
    scratch_rows: &mut usize,
) -> DfResult<()> {
    for (index, base) in unprobed.drain(..) {
        probe_batch(
            facts,
            partial,
            plan,
            &held[index],
            system_columns,
            base,
            sets,
            scratch_rows,
        )?;
    }
    Ok(())
}

/// Send what a forwarding partition held while it waited for the sets, in
/// order, and release `waiting`'s charge for them; a holding partition keeps
/// them.
async fn send_held(
    forward: &Forward,
    memory: &Arc<WorkMemory>,
    waiting: &WorkMemory,
    held: &mut Vec<RecordBatch>,
) -> DfResult<()> {
    let Some(sender) = forward else {
        return Ok(());
    };
    for batch in held.drain(..) {
        sender.lock().await.send(batch, Arc::clone(memory)).await?;
    }
    waiting.release_work();
    Ok(())
}

/// The shared unique holders sized from the source's row estimate once it has
/// one, each check's tables charged to `facts` only while the pool has room
/// ([`WorkMemory::grow_if_free`]); `true` once sized, by any partition.
fn size_unique_holders(
    facts: &WorkMemory,
    plan: &SweepPlan,
    source: &dyn ExecutionPlan,
) -> DfResult<bool> {
    let Some(rows) = source
        .downcast_ref::<CsvSourceExec>()
        .and_then(CsvSourceExec::rows_estimate)
    else {
        return Ok(false);
    };
    let rows = usize::try_from(rows).unwrap_or(usize::MAX);
    let mut failed = None;
    plan.size_unique_holders(
        rows,
        WorkMemory::entry_bytes::<(Vec<String>, String)>(),
        &mut |bytes| {
            let table = match facts.child("load unique holders table") {
                Ok(table) => table,
                Err(error) => {
                    failed = Some(error);
                    return None;
                }
            };
            match table.grow_if_free(bytes) {
                Ok(true) => Some(Box::new(table) as Box<dyn Send>),
                Ok(false) => None,
                Err(error) => {
                    failed = Some(error);
                    None
                }
            }
        },
    );
    match failed {
        Some(error) => Err(error),
        None => Ok(true),
    }
}

/// Probe one held batch; the partition's parse scratch grows under `facts`
/// to the largest batch it has probed.
#[allow(clippy::too_many_arguments)]
fn probe_batch(
    facts: &WorkMemory,
    partial: &mut SweepPartial,
    plan: &SweepPlan,
    batch: &RecordBatch,
    system_columns: SystemColumns,
    base: u64,
    sets: &SweptSets,
    scratch_rows: &mut usize,
) -> DfResult<()> {
    let rows = batch.num_rows();
    if rows > *scratch_rows {
        facts.grow(parse_scratch_bytes(rows - *scratch_rows).map_err(external)?)?;
        *scratch_rows = rows;
    }
    partial
        .probe_endpoints(plan, batch, system_columns, base, sets)
        .map_err(external)
}

/// The judged unique holders freed shard by shard on workers at once, while
/// the partitions' charges still cover them.
async fn release_holders(memory: &Arc<WorkMemory>, plan: &SweepPlan) -> DfResult<()> {
    futures::future::join_all(plan.release_unique_holders().into_iter().map(|maps| {
        memory.blocking(move |_| async move {
            drop(maps);
            Ok::<(), datafusion::error::DataFusionError>(())
        })
    }))
    .await
    .into_iter()
    .collect::<DfResult<Vec<()>>>()?;
    Ok(())
}

fn admit_validation(
    memory: &WorkMemory,
    judge: &LoadJudge,
    batches: &[RecordBatch],
) -> DfResult<()> {
    let sc = judge.catalog.system_columns;
    let replacement = judge.mode == LoadMode::Overwrite;
    let needs_ids = judge
        .checks
        .iter()
        .any(|spec| matches!(spec.check, ValidateCheck::Unique { .. }) && spec.consults.is_some());
    for batch in batches {
        let rows = batch.num_rows();
        let column = |name: &str| {
            batch.column_by_name(name).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "validation batch missing column '{name}'"
                ))
            })
        };
        let id_bytes = string_payload_bytes(column(sc.id)?);
        if needs_ids {
            memory.entries::<String>(rows)?;
            memory.string(id_bytes)?;
        }
        for spec in &judge.checks {
            if let ValidateCheck::Unique { columns, .. } = &spec.check {
                memory.entries::<(Vec<String>, String)>(rows)?;
                if replacement {
                    memory.grow(
                        rows.saturating_mul(columns.len())
                            .saturating_mul(std::mem::size_of::<String>()),
                    )?;
                    memory.string(id_bytes)?;
                    let names = columns
                        .iter()
                        .fold(judge.table.len().saturating_add(128), |sum, name| {
                            sum.saturating_add(name.len().saturating_mul(2))
                        });
                    memory.string(
                        string_max_bytes(column(sc.id)?)
                            .saturating_mul(4)
                            .saturating_add(names),
                    )?;
                } else {
                    memory.entries::<(String, (Vec<String>, Vec<ScalarValue>))>(rows)?;
                    memory.entries::<(&Vec<String>, &String)>(rows)?;
                    memory
                        .entries::<String>(rows.saturating_mul(columns.len()).saturating_mul(2))?;
                    memory.entries::<ScalarValue>(rows.saturating_mul(columns.len()))?;
                    memory.string(id_bytes.saturating_mul(2))?;
                }
                for name in columns {
                    let values = column(name)?;
                    let typed_bytes = string_payload_bytes(values);
                    let canonical_bytes = if matches!(
                        values.data_type(),
                        arrow_schema::DataType::Utf8
                            | arrow_schema::DataType::LargeUtf8
                            | arrow_schema::DataType::Utf8View
                    ) {
                        typed_bytes
                    } else {
                        values
                            .len()
                            .saturating_sub(values.null_count())
                            .saturating_mul(512)
                    };
                    if replacement {
                        let canonical_max = if matches!(
                            values.data_type(),
                            arrow_schema::DataType::Utf8
                                | arrow_schema::DataType::LargeUtf8
                                | arrow_schema::DataType::Utf8View
                        ) {
                            string_max_bytes(values)
                        } else if values.len() != values.null_count() {
                            512
                        } else {
                            0
                        };
                        memory.string(canonical_max.saturating_mul(3))?;
                    }
                    memory.string(if replacement {
                        canonical_bytes
                    } else {
                        canonical_bytes
                            .saturating_mul(2)
                            .saturating_add(typed_bytes)
                    })?;
                    if spec.consults.is_some() {
                        memory.entries::<(Vec<String>, Vec<ScalarValue>)>(rows)?;
                        memory.entries::<String>(rows)?;
                        memory.entries::<ScalarValue>(rows)?;
                        memory.string(canonical_bytes.saturating_add(typed_bytes))?;
                    }
                }
            }
        }
        if judge.checks.iter().any(|spec| {
            matches!(&spec.check, ValidateCheck::EdgeRi { node_type, .. } if !int_keyed(&judge.catalog, node_type))
        }) {
            let src_bytes = string_payload_bytes(column(sc.src)?);
            let dst_bytes = string_payload_bytes(column(sc.dst)?);
            memory.entries::<(String, String, String)>(rows)?;
            memory.entries::<String>(rows.saturating_mul(4))?;
            memory.entries::<Expr>(rows.saturating_mul(2))?;
            memory.string(id_bytes)?;
            memory.string(src_bytes.saturating_add(dst_bytes).saturating_mul(4))?;
        }
        if judge
            .checks
            .iter()
            .any(|spec| matches!(spec.check, ValidateCheck::Cardinality { .. }))
        {
            let src_bytes = string_payload_bytes(column(sc.src)?);
            memory.entries::<(String, String)>(rows)?;
            memory.entries::<String>(rows.saturating_mul(3))?;
            memory.entries::<(&String, &String)>(rows)?;
            memory.entries::<&String>(rows.saturating_mul(2))?;
            memory.string(
                id_bytes
                    .saturating_mul(2)
                    .saturating_add(src_bytes.saturating_mul(3)),
            )?;
        }
    }
    Ok(())
}

fn string_max_bytes(column: &ArrayRef) -> usize {
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values.iter().flatten().map(str::len).max().unwrap_or(0);
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return values.iter().flatten().map(str::len).max().unwrap_or(0);
    }
    if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
        return values.iter().flatten().map(str::len).max().unwrap_or(0);
    }
    0
}

fn string_payload_bytes(column: &ArrayRef) -> usize {
    if let Some(values) = column.as_any().downcast_ref::<StringArray>() {
        return values
            .iter()
            .flatten()
            .fold(0usize, |sum, value| sum.saturating_add(value.len()));
    }
    if let Some(values) = column.as_any().downcast_ref::<LargeStringArray>() {
        return values
            .iter()
            .flatten()
            .fold(0usize, |sum, value| sum.saturating_add(value.len()));
    }
    if let Some(values) = column.as_any().downcast_ref::<StringViewArray>() {
        return values
            .iter()
            .flatten()
            .fold(0usize, |sum, value| sum.saturating_add(value.len()));
    }
    0
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Mutex;

    use datafusion::execution::memory_pool::{
        GreedyMemoryPool, MemoryConsumer, MemoryPool, MemoryReservation,
    };
    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::ir::lower_load;
    use omnigraph_compiler::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};
    use omnigraph_compiler::schema::parser::parse_schema;
    use omnigraph_compiler::settings::SessionSettings;
    use omnigraph_planner::{ConsultedImage, EndpointProbe, ValidateAccess, ValidateRemoval};

    use super::super::csv_source::CSV_SLICE_METADATA;
    use super::super::fixtures;
    use super::*;
    use crate::loader::csv::{
        CSV_READ_BATCH_ROWS, CsvBytes, CsvLoadRequest, NormalizeStep, RangeScanner, ResolvedCsv,
        target_arrow_schema,
    };

    const PERSON: &str = "node Person {\n  key: I64 @key\n  score: F64\n}\n";

    fn request() -> CsvLoadRequest {
        CsvLoadRequest {
            source: "file:///input.csv".to_string(),
            target: LoadTarget::Node("Person".to_string()),
            mode: LoadMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        }
    }

    fn catalog(schema: &str) -> Arc<Catalog> {
        Arc::new(build_catalog(&parse_schema(schema).unwrap()).unwrap())
    }

    /// The slice `record` sits in when `source` is cut at 64 bytes by one
    /// partition.
    async fn slice_of_record(schema: &str, source: &str, record: usize) -> usize {
        let size = source.len() as u64;
        let resolved =
            ResolvedCsv::resolve(&catalog(schema), &request(), source.as_bytes(), size).unwrap();
        let mut scanner = RangeScanner::new(
            bytes(source),
            b',',
            resolved.records_start(),
            size,
            64,
            false,
        );
        let mut first_record = 1;
        let mut index = 0;
        while let Some(slice) = scanner
            .next_slice(&request(), &mut |_| Ok(()))
            .await
            .unwrap()
        {
            if (first_record..first_record + slice.rows).contains(&record) {
                return index;
            }
            first_record += slice.rows;
            index += 1;
        }
        panic!("record {record} is past the source's {first_record} records");
    }

    fn bytes(source: &str) -> Arc<CsvBytes> {
        Arc::new(CsvBytes::Memory(Arc::from(source.as_bytes())))
    }

    /// `source` cut into 64-byte slices, parsed by `partitions` and judged by
    /// the breaker as an overwrite of a fresh graph of `schema`.
    async fn validate(
        schema: &str,
        source: &str,
        partitions: usize,
    ) -> (Arc<CsvSourceExec>, DfResult<Vec<RecordBatch>>) {
        validate_as(schema, source, partitions, false).await
    }

    /// [`validate`] with the shape chosen: the breaker under a removal scan,
    /// the streaming operator under `removal: None`.
    async fn validate_as(
        schema: &str,
        source: &str,
        partitions: usize,
        streams: bool,
    ) -> (Arc<CsvSourceExec>, DfResult<Vec<RecordBatch>>) {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Session::from_defaults(
            Arc::new(
                Omnigraph::init(dir.path().to_str().unwrap(), schema)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        let catalog = db.catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = Arc::new(CsvSourceExec::new(
            request(),
            bytes(source),
            Arc::clone(&catalog),
            schema,
            partitions,
            64,
            Arc::default(),
        ));
        let stmt = LoadStmt {
            source: "file:///input.csv".to_string(),
            target: LoadTarget::Node("Person".to_string()),
            mode: LoadStmtMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        };
        let checks = lower_load(&catalog, &stmt)
            .checks
            .into_iter()
            .map(|check| ValidateSpec {
                access: Some(ValidateAccess::Delta),
                consults: check.consults.map(|table| ConsultedImage {
                    table,
                    version: None,
                }),
                check: check.check,
                probe: None,
            })
            .collect();
        let judge = LoadJudge {
            db: Arc::clone(db.db()),
            table: "node:Person".to_string(),
            checks,
            removal: if streams {
                ValidateRemoval::None
            } else {
                ValidateRemoval::Scan
            },
            mode: LoadMode::Overwrite,
            write_budget: WriteBudget::from_settings(db.settings()),
            snapshot: db.snapshot().await,
            catalog,
        };
        let validate =
            ValidateExec::new(Arc::clone(&exec) as Arc<dyn ExecutionPlan>, judge, streams);
        let (_pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
        let judged = validate
            .execute(0, ctx)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<DfResult<Vec<_>>>();
        (exec, judged)
    }

    fn keys(batches: &[RecordBatch]) -> Vec<i64> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("key")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// The streaming shape sends each partition's batches on as they pass,
    /// slice tag kept for the sink's ordering: every row once, each partition
    /// in source order; a case sees the loaded rows, never the batches.
    #[tokio::test]
    async fn partitions_stream_their_rows_tagged_by_slice() {
        let mut source = String::from("key,score\n");
        for row in 0..1000 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let (exec, streamed) = validate_as(PERSON, &source, 3, true).await;
        let batches = streamed.unwrap();
        assert!(
            batches
                .iter()
                .all(|batch| batch.schema().metadata().contains_key(CSV_SLICE_METADATA))
        );
        let mut per_partition: std::collections::BTreeMap<usize, Vec<i64>> =
            std::collections::BTreeMap::new();
        for batch in &batches {
            per_partition
                .entry(slice_of(batch) >> 32)
                .or_default()
                .extend(keys(std::slice::from_ref(batch)));
        }
        assert_eq!(per_partition.len(), 3, "{per_partition:?}");
        for rows in per_partition.values() {
            assert!(rows.windows(2).all(|pair| pair[0] < pair[1]), "{rows:?}");
        }
        let mut all: Vec<i64> = per_partition.into_values().flatten().collect();
        all.sort_unstable();
        assert_eq!(all, (0..1000).collect::<Vec<i64>>());
        assert!(exec.failed_slices().is_empty());
    }

    /// Whatever partition claimed each slice, the breaker hands the held rows
    /// on in source order under the plain schema; a case sees the loaded
    /// rows, never the batches or the claim.
    #[tokio::test]
    async fn partitions_drain_back_into_source_order() {
        let mut source = String::from("key,score\n");
        for row in 0..1000 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let (exec, drained) = validate(PERSON, &source, 3).await;
        let batches = drained.unwrap();
        assert_eq!(keys(&batches), (0..1000).collect::<Vec<i64>>());
        assert!(
            batches
                .iter()
                .all(|batch| !batch.schema().metadata().contains_key(CSV_SLICE_METADATA))
        );
        assert!(exec.executed_slices().len() > 3);
        assert!(exec.failed_slices().is_empty());
    }

    /// Two bad records at the same normalization step in two slices: one
    /// partition normalizes every slice and names the lower record; three
    /// partitions refuse and mark the run for the loader's one-partition rerun.
    #[tokio::test]
    async fn the_lowest_failed_slice_names_the_refusal() {
        let mut source = String::from("key,score\n");
        for row in 0..40 {
            if row == 11 || row == 27 {
                writeln!(&mut source, "{row:03},").unwrap();
            } else {
                writeln!(&mut source, "{row:03},1.5").unwrap();
            }
        }
        assert_eq!(
            (
                slice_of_record(PERSON, &source, 12).await,
                slice_of_record(PERSON, &source, 28).await
            ),
            (1, 3)
        );

        let (exec, drained) = validate(PERSON, &source, 1).await;
        let error = drained.unwrap_err();
        assert!(
            error.to_string().contains("record 12 has an empty field"),
            "{error}"
        );
        let failed = exec.failed_slices();
        assert_eq!(failed.iter().map(|failed| failed.slice).min(), Some(1));
        assert!(
            failed.iter().all(|failed| [1, 3].contains(&failed.slice)),
            "{failed:?}"
        );
        assert!(!exec.refused());

        let (exec, drained) = validate(PERSON, &source, 3).await;
        assert!(drained.is_err());
        assert!(exec.refused(), "three partitions mark the rerun");
    }

    /// Two properties refused in two slices, the later-declared one in the
    /// earlier slice: one partition keeps normalizing past a refusal and
    /// names the earlier-declared property's record; two refuse for the rerun.
    #[tokio::test]
    async fn the_first_declared_property_names_the_normalization_refusal() {
        const SCHEMA: &str = "node Person {\n  key: I64 @key\n  a: Bool\n  b: Bool\n}\n";
        let mut source = String::from("key,a,b\n");
        for row in 0..40 {
            let (a, b) = match row {
                8 => ("true", "nope"),
                20 => ("nope", "true"),
                _ => ("true", "true"),
            };
            writeln!(&mut source, "{row:03},{a},{b}").unwrap();
        }
        assert!(
            slice_of_record(SCHEMA, &source, 9).await < slice_of_record(SCHEMA, &source, 21).await
        );
        let (exec, judged) = validate(SCHEMA, &source, 1).await;
        let error = judged.unwrap_err().to_string();
        assert!(
            error.contains("record 21, property 'a': 'nope' is not `true` or `false`"),
            "{error}"
        );
        let failed = exec.failed_slices();
        assert!(
            failed.iter().any(|failed| failed.normalize
                == Some(NormalizeStep {
                    step: 2,
                    record: 21
                })),
            "{failed:?}"
        );
        assert!(!exec.refused());

        let (exec, judged) = validate(SCHEMA, &source, 2).await;
        assert!(judged.is_err());
        assert!(exec.refused(), "two partitions mark the rerun");
    }

    /// Two `@range` constraints (the catalog sorts them by property: `age`
    /// before `score`), the second violated in an earlier slice than the first:
    /// the first-listed constraint is named, from one partition or two.
    #[tokio::test]
    async fn the_first_listed_constraint_names_the_value_refusal() {
        const SCHEMA: &str = "node Person {\n  key: I64 @key\n  score: F64\n  age: I64\n  @range(score, 0.0..10.0)\n  @range(age, 0..150)\n}\n";
        let dir = tempfile::tempdir().unwrap();
        let graph = Omnigraph::init(dir.path().to_str().unwrap(), SCHEMA)
            .await
            .unwrap();
        assert_eq!(
            graph.catalog().node_types["Person"]
                .range_constraints
                .iter()
                .map(|rc| rc.property.as_str())
                .collect::<Vec<_>>(),
            ["age", "score"]
        );
        let mut source = String::from("key,score,age\n");
        for row in 0..40 {
            let (score, age) = match row {
                8 => ("99", 30),
                20 => ("1.5", 999),
                _ => ("1.5", 30),
            };
            writeln!(&mut source, "{row:03},{score},{age}").unwrap();
        }
        assert!(
            slice_of_record(SCHEMA, &source, 9).await < slice_of_record(SCHEMA, &source, 21).await
        );
        for partitions in [1, 2] {
            let (_, judged) = validate(SCHEMA, &source, partitions).await;
            let error = judged.unwrap_err().to_string();
            assert!(
                error.contains("@range violation on Person.age: value 999 > max 150"),
                "{partitions} partitions: {error}"
            );
        }
    }

    /// Two keys each repeated once, the repeats far apart in the source: the
    /// refusal names the repeat that comes first in source order, from one
    /// partition or three, whatever partition swept each slice.
    #[tokio::test]
    async fn the_first_repeated_key_in_source_order_is_the_refusal() {
        let mut source = String::from("key,score\n");
        for row in 0..600 {
            let key = match row {
                300 => 20,
                500 => 5,
                _ => row,
            };
            writeln!(&mut source, "{key},1.5").unwrap();
        }
        for partitions in [1, 3] {
            let (_, judged) = validate(PERSON, &source, partitions).await;
            let error = judged.unwrap_err().to_string();
            assert!(
                error.contains("value '20' held by '20' and '20'"),
                "{partitions} partitions: {error}"
            );
        }
    }

    const KEYED_GRAPH: &str = "node Person {\n  key: I64 @key\n}\nedge Knows: Person -> Person\n";

    /// An edge batch of `slice`, the three system columns only.
    fn knows_batch(sc: SystemColumns, slice: usize, rows: &[(&str, &str, &str)]) -> RecordBatch {
        let mut metadata = std::collections::HashMap::new();
        metadata.insert(CSV_SLICE_METADATA.to_string(), slice.to_string());
        let schema = Arc::new(
            arrow_schema::Schema::new(vec![
                arrow_schema::Field::new(sc.id, arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new(sc.src, arrow_schema::DataType::Utf8, false),
                arrow_schema::Field::new(sc.dst, arrow_schema::DataType::Utf8, false),
            ])
            .with_metadata(metadata),
        );
        let column = |pick: fn(&(&str, &str, &str)) -> String| -> ArrayRef {
            Arc::new(StringArray::from_iter_values(rows.iter().map(pick)))
        };
        RecordBatch::try_new(
            schema,
            vec![
                column(|row| row.0.to_string()),
                column(|row| row.1.to_string()),
                column(|row| row.2.to_string()),
            ],
        )
        .unwrap()
    }

    /// A partition's input: `batches`, then `ended` fires as the stream
    /// closes, so a test can publish the key sets only after every row of
    /// the partition is held.
    fn partition_input(
        batches: Vec<RecordBatch>,
        ended: tokio::sync::oneshot::Sender<()>,
    ) -> SendableRecordBatchStream {
        let schema = batches[0].schema();
        let stream = futures::stream::iter(batches.into_iter().map(Ok)).chain(
            futures::stream::once(async move {
                let _ = ended.send(());
            })
            .flat_map(|()| futures::stream::empty()),
        );
        Box::pin(datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream))
    }

    /// A graph of `KEYED_GRAPH` holding Persons 1, 2 and 3.
    async fn keyed_graph(dir: &tempfile::TempDir) -> crate::Session {
        let db = crate::Session::from_defaults(
            Arc::new(
                Omnigraph::init(dir.path().to_str().unwrap(), KEYED_GRAPH)
                    .await
                    .unwrap(),
            ),
            SessionSettings::default(),
        );
        db.load_jsonl(
            "{\"type\":\"Person\",\"data\":{\"key\":1}}\n{\"type\":\"Person\",\"data\":{\"key\":2}}\n{\"type\":\"Person\",\"data\":{\"key\":3}}\n",
            LoadMode::Overwrite,
        )
        .await
        .unwrap();
        db
    }

    fn knows_request() -> CsvLoadRequest {
        CsvLoadRequest {
            source: "file:///knows.csv".to_string(),
            target: LoadTarget::Edge("Knows".to_string()),
            mode: LoadMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        }
    }

    /// A `Knows` overwrite's checks under `catalog`, both endpoint checks swept.
    fn swept_checks(catalog: &Catalog) -> Vec<ValidateSpec> {
        let stmt = LoadStmt {
            source: "file:///knows.csv".to_string(),
            target: LoadTarget::Edge("Knows".to_string()),
            mode: LoadStmtMode::Overwrite,
            delimiter: b',',
            header: true,
            columns: None,
        };
        lower_load(catalog, &stmt)
            .checks
            .into_iter()
            .map(|check| {
                let swept = matches!(check.check, ValidateCheck::EdgeRi { .. });
                ValidateSpec {
                    access: Some(if check.consults.is_some() {
                        ValidateAccess::Scan
                    } else {
                        ValidateAccess::Delta
                    }),
                    consults: check.consults.map(|table| ConsultedImage {
                        table,
                        version: None,
                    }),
                    check: check.check,
                    probe: swept.then_some(EndpointProbe::Swept),
                }
            })
            .collect()
    }

    fn knows_judge(db: &crate::Session, checks: &[ValidateSpec], snapshot: &Snapshot) -> LoadJudge {
        LoadJudge {
            db: Arc::clone(db.db()),
            table: "edge:Knows".to_string(),
            checks: checks.to_vec(),
            removal: ValidateRemoval::None,
            mode: LoadMode::Overwrite,
            write_budget: WriteBudget::from_settings(db.settings()),
            snapshot: snapshot.clone(),
            catalog: db.catalog(),
        }
    }

    /// The sets arrive after both partitions hold every batch, then before
    /// the first: either way the refusal is the earliest miss in source
    /// order, src before dst; a `.gqt` case cannot order loader and parse.
    #[tokio::test]
    async fn held_batches_are_probed_once_the_sets_arrive() {
        let dir = tempfile::tempdir().unwrap();
        let db = keyed_graph(&dir).await;
        let catalog = db.catalog();
        let sc = catalog.system_columns;
        let checks = swept_checks(&catalog);
        let snapshot = db.snapshot().await;
        let (sets, _retained, _transient) =
            admit_swept_sets("edge:Knows", &checks, &snapshot, true, &mut |_| {
                Ok(Box::new(()))
            })
            .await
            .unwrap()
            .load(sc)
            .await
            .unwrap();
        let sets = Arc::new(sets);
        let judge = Arc::new(knows_judge(&db, &checks, &snapshot));
        let plan = Arc::new(SweepPlan::new("edge:Knows", &checks, true, 2));
        assert!(plan.sweeps_endpoints());
        let later_slice_missing_dst_at_row_1 = vec![
            knows_batch(sc, 1, &[("e4", "3", "1"), ("e5", "2", "9")]),
            knows_batch(sc, 1, &[("e6", "1", "1")]),
        ];
        let earlier_slice_missing_src_and_dst_at_row_2 = vec![
            knows_batch(sc, 0, &[("e1", "1", "2"), ("e2", "2", "3")]),
            knows_batch(sc, 0, &[("e3", "7", "8")]),
        ];
        for publish_first in [false, true] {
            let (publish, published) = watch::channel(None::<Arc<SweptSets>>);
            if publish_first {
                publish.send_replace(Some(Arc::clone(&sets)));
            }
            let (_pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
            let metrics = ExecutionPlanMetricsSet::new();
            let memory = work(ctx, "ValidateExec", &metrics).unwrap();
            let (ended_a, a_ended) = tokio::sync::oneshot::channel();
            let (ended_b, b_ended) = tokio::sync::oneshot::channel();
            let source: Arc<dyn ExecutionPlan> =
                Arc::new(datafusion::physical_plan::empty::EmptyExec::new(Arc::new(
                    arrow_schema::Schema::empty(),
                )));
            let sweep = |batches: Vec<RecordBatch>, ended| {
                let plan = Arc::clone(&plan);
                let judge = Arc::clone(&judge);
                let published = published.clone();
                let memory = Arc::clone(&memory);
                let source = Arc::clone(&source);
                async move {
                    memory
                        .blocking(move |memory| {
                            sweep_partition(
                                partition_input(batches, ended),
                                memory,
                                source,
                                plan,
                                judge,
                                published,
                                None,
                            )
                        })
                        .await
                }
            };
            let publisher = async {
                a_ended.await.unwrap();
                b_ended.await.unwrap();
                if !publish_first {
                    publish.send_replace(Some(Arc::clone(&sets)));
                }
            };
            let (a, b, ()) = futures::join!(
                sweep(later_slice_missing_dst_at_row_1.clone(), ended_a),
                sweep(earlier_slice_missing_src_and_dst_at_row_2.clone(), ended_b),
                publisher
            );
            let a = a.unwrap();
            let b = b.unwrap();
            let extracted = Extracted::prepare(&plan, vec![a.partial, b.partial]);
            let held: Vec<RecordBatch> = b.held.into_iter().chain(a.held).collect();
            let refused = judge_load_swept(
                "edge:Knows",
                &checks,
                ValidateRemoval::None,
                &catalog,
                &snapshot,
                LoadMode::Overwrite,
                &held,
                a.rows + b.rows,
                extracted,
                &mut LoadAdmission {
                    phase: None,
                    bytes: None,
                    reuse: None,
                    removed: &mut |_| Ok(()),
                    retained: &mut |_| Ok(()),
                },
            )
            .await
            .unwrap_err()
            .to_string();
            assert!(
                refused.contains("'7' not found in Person"),
                "publish_first={publish_first}: {refused}"
            );
        }
    }

    /// A budget the sets do not fit is refused before any partition is polled,
    /// and the scan input and build scratch stay charged until the operator
    /// releases them after the drain: a `.gqt` case sees one `actual` per replay.
    #[tokio::test]
    async fn the_sets_are_admitted_before_the_drain_and_released_after_it() {
        let dir = tempfile::tempdir().unwrap();
        let db = keyed_graph(&dir).await;
        let catalog = db.catalog();
        let checks = swept_checks(&catalog);
        let snapshot = db.snapshot().await;
        let plan = Arc::new(SweepPlan::new("edge:Knows", &checks, true, 2));
        let three_keys_scan_input = 3 * 8;
        let three_keys_in_four_slots_retained = (4 + 1) * 8 + 3 * 8;
        let four_slots_build_scratch = 4 * 8;
        let whole_set =
            three_keys_scan_input + three_keys_in_four_slots_retained + four_slots_build_scratch;

        let exec = Arc::new(CsvSourceExec::new(
            knows_request(),
            bytes("from,to\n1,2\n2,3\n3,1\n"),
            Arc::clone(&catalog),
            target_arrow_schema(&catalog, &LoadTarget::Edge("Knows".to_string())).unwrap(),
            2,
            64,
            Arc::default(),
        ));
        let validate = ValidateExec::new(
            Arc::clone(&exec) as Arc<dyn ExecutionPlan>,
            knows_judge(&db, &checks, &snapshot),
            false,
        );
        let (_pool, ctx) = fixtures::context(whole_set - 1, CSV_READ_BATCH_ROWS);
        let refused = validate
            .execute(0, ctx)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<DfResult<Vec<_>>>()
            .unwrap_err()
            .to_string();
        assert!(refused.contains("query_memory_bytes"), "{refused}");
        let drained = validate
            .metrics()
            .and_then(|metrics| metrics.sum_by_name("input_rows"))
            .map(|metric| metric.as_usize());
        assert_eq!(
            drained,
            Some(0),
            "no partition was polled before the sets were refused"
        );
        assert!(exec.executed_slices().is_empty());

        let (pool, ctx) = fixtures::context(64 << 20, CSV_READ_BATCH_ROWS);
        let metrics = ExecutionPlanMetricsSet::new();
        let memory = work(ctx, "ValidateExec", &metrics).unwrap();
        let judge = Arc::new(knows_judge(&db, &checks, &snapshot));
        let before = pool.reserved();
        let admitted = admit_sets(&memory, &judge, &plan).await.unwrap();
        assert_eq!(pool.reserved() - before, whole_set);
        let (publish, published) = watch::channel(None::<Arc<SweptSets>>);
        let (retained, transient) = load_sets(Arc::clone(&memory), judge, admitted, publish)
            .await
            .unwrap();
        assert!(published.borrow().is_some());
        assert_eq!(
            pool.reserved() - before,
            whole_set,
            "the scan input and build scratch stay charged until the operator releases them"
        );
        drop(transient);
        assert_eq!(pool.reserved() - before, three_keys_in_four_slots_retained);
        drop(retained);
        assert_eq!(pool.reserved(), before);
    }

    /// A pool recording the consumer and size of every charge in the order it
    /// saw them, over a greedy pool of `limit` bytes.
    #[derive(Debug)]
    struct ChargeLedger {
        inner: GreedyMemoryPool,
        charges: Mutex<Vec<(String, usize)>>,
    }

    impl ChargeLedger {
        fn new(limit: usize) -> Self {
            Self {
                inner: GreedyMemoryPool::new(limit),
                charges: Mutex::new(Vec::new()),
            }
        }

        fn record(&self, reservation: &MemoryReservation, additional: usize) {
            self.charges
                .lock()
                .unwrap()
                .push((reservation.consumer().name().to_owned(), additional));
        }

        fn charges(&self) -> Vec<(String, usize)> {
            self.charges.lock().unwrap().clone()
        }
    }

    impl fmt::Display for ChargeLedger {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("charge ledger")
        }
    }

    impl MemoryPool for ChargeLedger {
        fn name(&self) -> &str {
            "charge ledger"
        }

        fn register(&self, consumer: &MemoryConsumer) {
            self.inner.register(consumer);
        }

        fn unregister(&self, consumer: &MemoryConsumer) {
            self.inner.unregister(consumer);
        }

        fn grow(&self, reservation: &MemoryReservation, additional: usize) {
            self.record(reservation, additional);
            self.inner.grow(reservation, additional);
        }

        fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
            self.inner.shrink(reservation, shrink);
        }

        fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> DfResult<()> {
            self.record(reservation, additional);
            self.inner.try_grow(reservation, additional)
        }

        fn reserved(&self) -> usize {
            self.inner.reserved()
        }
    }

    /// Every key-set charge precedes the parse's first charge: the sets are
    /// admitted before the source partitions are executed, a thread order
    /// GQT cannot observe (as `partitions_admit_their_source_before_the_first_poll`).
    #[tokio::test]
    async fn the_sets_are_charged_before_the_source_partitions_are_executed() {
        let dir = tempfile::tempdir().unwrap();
        let db = keyed_graph(&dir).await;
        let catalog = db.catalog();
        let checks = swept_checks(&catalog);
        let snapshot = db.snapshot().await;
        let exec = Arc::new(CsvSourceExec::new(
            knows_request(),
            bytes("from,to\n1,2\n2,3\n3,1\n"),
            Arc::clone(&catalog),
            target_arrow_schema(&catalog, &LoadTarget::Edge("Knows".to_string())).unwrap(),
            2,
            64,
            Arc::default(),
        ));
        let validate = ValidateExec::new(
            Arc::clone(&exec) as Arc<dyn ExecutionPlan>,
            knows_judge(&db, &checks, &snapshot),
            false,
        );
        let ledger = Arc::new(ChargeLedger::new(64 << 20));
        let ctx = fixtures::context_over(
            Arc::clone(&ledger) as Arc<dyn MemoryPool>,
            64 << 20,
            CSV_READ_BATCH_ROWS,
        );
        let judged = validate
            .execute(0, ctx)
            .unwrap()
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<DfResult<Vec<_>>>()
            .unwrap();
        assert_eq!(judged.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
        let charges = ledger.charges();
        let first = |consumer: &str| {
            charges
                .iter()
                .position(|(name, _)| name == consumer)
                .unwrap_or_else(|| panic!("no charge by {consumer}: {charges:?}"))
        };
        let last_set = charges
            .iter()
            .rposition(|(name, _)| name == "load integer endpoint keys")
            .unwrap_or_else(|| panic!("no key-set charge: {charges:?}"));
        assert!(last_set < first("CsvSourceExec"), "{charges:?}");
        assert!(last_set < first("CSV source bytes"), "{charges:?}");
        assert!(last_set < first("CSV decoding"), "{charges:?}");
    }
}
