//! A load's validation: a pipeline breaker that holds the whole delta from
//! every partition of its input, restores source order by slice, judges
//! every check of the plan in one pass over it, and streams the held batches
//! on only when the delta is admitted.

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
use omnigraph_planner::ValidateSpec;

use futures::StreamExt;

use super::csv_source::{CsvSourceExec, slice_of};
use super::external;
use super::memory::WorkMemory;
use super::shape::{Breaker, work};
use crate::db::{Omnigraph, Snapshot};
use crate::error::OmniError;
use crate::loader::LoadMode;
use crate::loader::read_reuse::LoadReadReuse;
use crate::storage_layer::{DeletedIdBudget, WriteBudget};
use crate::validate::load::{
    Extracted, LoadAdmission, LoadPhase, SweepPartial, SweepPlan, int_keyed, judge_load_swept,
    position,
};
use omnigraph_compiler::ir::ValidateCheck;

/// The `validated_rows` metric: the delta rows the operator judged.
pub(crate) const VALIDATED_ROWS: &str = "validated_rows";

/// What one validation judges against: the plan's checks, the statement's
/// mode, and the committed image the transaction pins.
pub(crate) struct LoadJudge {
    pub(crate) db: Arc<Omnigraph>,
    pub(crate) table: String,
    pub(crate) checks: Vec<ValidateSpec>,
    pub(crate) mode: LoadMode,
    pub(crate) write_budget: WriteBudget,
    pub(crate) snapshot: Snapshot,
    pub(crate) catalog: Arc<Catalog>,
}

pub(crate) struct ValidateExec {
    input: Arc<dyn ExecutionPlan>,
    judge: Arc<LoadJudge>,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl ValidateExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, judge: LoadJudge) -> Self {
        let schema = input.schema();
        Self {
            input,
            judge: Arc::new(judge),
            shape: Breaker::new(schema),
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
        Ok(Arc::new(Self {
            shape: Breaker::new(input.schema()),
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
        let inputs = (0..partitions)
            .map(|partition| self.input.execute(partition, Arc::clone(&ctx)))
            .collect::<DfResult<Vec<_>>>()?;
        let source = Arc::clone(&self.input);
        let schema = self.input.schema();
        let memory = work(ctx, "ValidateExec", &self.metrics)?;
        memory.metric("input_rows", 0);
        memory.metric(VALIDATED_ROWS, 0);
        let judge = Arc::clone(&self.judge);
        let plan = Arc::new(SweepPlan::new(
            &judge.table,
            &judge.checks,
            judge.mode == LoadMode::Overwrite,
            partitions,
        ));
        Ok(self.shape.held(
            memory,
            &self.metrics,
            move |memory| async move {
                let (batches, partials, facts) =
                    drain_and_sweep(inputs, &memory, source.as_ref(), &schema, &plan, &judge)
                        .await?;
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                let extracted = merge_partials(&memory, &plan, &judge.table, partials).await?;
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
                        memory.admit_read_reuse().and_then(|admitted| {
                            LoadReadReuse::new(&judge.db, &judge.snapshot, admitted)
                        })
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
                        &judge.catalog,
                        &judge.snapshot,
                        judge.mode,
                        &batches,
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
                drop(facts);
                memory.metric(VALIDATED_ROWS, rows);
                Ok(batches)
            },
            move |batches, memory, sender| async move {
                for batch in batches {
                    sender.send(batch, Arc::clone(&memory)).await?;
                }
                Ok(())
            },
        ))
    }
}

/// Every input partition drained and swept at once ([`sweep_partition`]), the
/// held batches sorted back into slice order under `schema`; the refusal is the
/// lowest failed slice's (a sliceless failure first), whatever partition met it.
async fn drain_and_sweep(
    inputs: Vec<SendableRecordBatchStream>,
    memory: &Arc<WorkMemory>,
    source: &dyn ExecutionPlan,
    schema: &arrow_schema::SchemaRef,
    plan: &Arc<SweepPlan>,
    judge: &Arc<LoadJudge>,
) -> DfResult<(Vec<RecordBatch>, Vec<SweepPartial>, Vec<WorkMemory>)> {
    let drained = futures::future::join_all(inputs.into_iter().map(|input| {
        let plan = Arc::clone(plan);
        let judge = Arc::clone(judge);
        memory.blocking(move |memory| sweep_partition(input, memory, plan, judge))
    }))
    .await;
    let mut batches = Vec::new();
    let mut partials = Vec::new();
    let mut facts = Vec::new();
    let mut failures = Vec::new();
    for (partition, result) in drained.into_iter().enumerate() {
        match result {
            Ok((held, partial, held_facts)) => {
                batches.extend(held);
                partials.push(partial);
                facts.push(held_facts);
            }
            Err(error) => failures.push((partition, error)),
        }
    }
    if !failures.is_empty() {
        let failed = source
            .downcast_ref::<CsvSourceExec>()
            .map(CsvSourceExec::failed_slices)
            .unwrap_or_default();
        let slice_of_partition = |partition: usize| {
            failed
                .iter()
                .filter(|(_, failed_partition)| *failed_partition == partition)
                .map(|(slice, _)| *slice)
                .min()
        };
        let winner = failures
            .iter()
            .position(|(partition, _)| slice_of_partition(*partition).is_none())
            .or_else(|| {
                failures
                    .iter()
                    .enumerate()
                    .min_by_key(|(_, (partition, _))| slice_of_partition(*partition))
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
    Ok((batches, partials, facts))
}

/// One partition's input held and swept as it arrives: every batch is held
/// under `memory`, its facts admitted under the returned child and extracted
/// into the partial at the batch's position in its slice.
async fn sweep_partition(
    mut input: SendableRecordBatchStream,
    memory: Arc<WorkMemory>,
    plan: Arc<SweepPlan>,
    judge: Arc<LoadJudge>,
) -> DfResult<(Vec<RecordBatch>, SweepPartial, WorkMemory)> {
    let facts = memory.child("load validation facts")?;
    let mut partial = SweepPartial::new(&plan);
    let mut held = Vec::new();
    let mut slice = usize::MAX;
    let mut offset = 0_u64;
    while let Some(batch) = input.next().await {
        let batch = batch?;
        memory.metric("input_rows", batch.num_rows());
        memory.hold(&batch)?;
        memory.entries::<RecordBatch>(1)?;
        let tagged = slice_of(&batch);
        if tagged != slice {
            slice = tagged;
            offset = 0;
        }
        admit_validation(&facts, &judge, std::slice::from_ref(&batch))?;
        partial
            .observe(
                &plan,
                &judge.catalog,
                &batch,
                judge.catalog.system_columns,
                position(slice, offset),
            )
            .map_err(external)?;
        offset += batch.num_rows() as u64;
        held.push(batch);
    }
    Ok((held, partial, facts))
}

/// The partitions' partials joined, each unique shard merged on its own
/// worker.
async fn merge_partials(
    memory: &Arc<WorkMemory>,
    plan: &SweepPlan,
    table: &str,
    partials: Vec<SweepPartial>,
) -> DfResult<Extracted> {
    let (merging, jobs) = Extracted::prepare(plan, partials);
    let table: Arc<str> = Arc::from(table);
    let shards = futures::future::join_all(jobs.into_iter().map(|job| {
        let table = Arc::clone(&table);
        memory.blocking(move |_| async move { Ok((job.check, job.run(&table))) })
    }))
    .await
    .into_iter()
    .collect::<DfResult<Vec<_>>>()?;
    Ok(merging.finish(shards))
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

    use omnigraph_compiler::catalog::build_catalog;
    use omnigraph_compiler::ir::lower_load;
    use omnigraph_compiler::query::ast::{LoadStmt, LoadStmtMode, LoadTarget};
    use omnigraph_compiler::schema::parser::parse_schema;
    use omnigraph_compiler::settings::SessionSettings;
    use omnigraph_planner::{ConsultedImage, ValidateAccess};

    use super::super::csv_source::CSV_SLICE_METADATA;
    use super::super::fixtures;
    use super::*;
    use crate::loader::csv::{
        CSV_READ_BATCH_ROWS, CsvLoadRequest, ResolvedCsv, target_arrow_schema,
    };
    use crate::table_store::CapturedSource;

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

    fn catalog() -> Arc<Catalog> {
        Arc::new(
            build_catalog(
                &parse_schema("node Person {\n  key: I64 @key\n  score: F64\n}\n").unwrap(),
            )
            .unwrap(),
        )
    }

    fn capture(source: &str) -> Arc<CapturedSource> {
        Arc::new(CapturedSource {
            bytes: Arc::from(source.as_bytes()),
            admission: None,
            size: source.len() as u64,
            e_tag: None,
            version: None,
        })
    }

    /// `source` cut into 64-byte slices, parsed by `partitions` and judged by
    /// the breaker as an overwrite of a fresh graph of the test schema.
    async fn validate(
        source: &str,
        partitions: usize,
    ) -> (Arc<CsvSourceExec>, DfResult<Vec<RecordBatch>>) {
        let dir = tempfile::tempdir().unwrap();
        let db = crate::Session::from_defaults(
            Arc::new(
                Omnigraph::init(
                    dir.path().to_str().unwrap(),
                    "node Person {\n  key: I64 @key\n  score: F64\n}\n",
                )
                .await
                .unwrap(),
            ),
            SessionSettings::default(),
        );
        let catalog = db.catalog();
        let schema = target_arrow_schema(&catalog, &request().target).unwrap();
        let exec = Arc::new(CsvSourceExec::new(
            request(),
            capture(source),
            Arc::clone(&catalog),
            schema,
            partitions,
            64,
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
            })
            .collect();
        let judge = LoadJudge {
            db: Arc::clone(db.db()),
            table: "node:Person".to_string(),
            checks,
            mode: LoadMode::Overwrite,
            write_budget: WriteBudget::from_settings(db.settings()),
            snapshot: db.snapshot().await,
            catalog,
        };
        let validate = ValidateExec::new(Arc::clone(&exec) as Arc<dyn ExecutionPlan>, judge);
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

    /// Whatever partition claimed each slice, the breaker hands the held rows
    /// on in source order under the plain schema; a case sees the loaded
    /// rows, never the batches or the claim.
    #[tokio::test]
    async fn partitions_drain_back_into_source_order() {
        let mut source = String::from("key,score\n");
        for row in 0..1000 {
            writeln!(&mut source, "{row},1.5").unwrap();
        }
        let (exec, drained) = validate(&source, 3).await;
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

    /// Two bad records in two slices: the slices are claimed in order and a
    /// claimed slice is parsed to its end, so the earlier slice always fails
    /// and its record is the refusal, on every run.
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
        let resolved = ResolvedCsv::resolve(&catalog(), &request(), source.as_bytes(), 64).unwrap();
        let slice_of_record = |record: usize| {
            resolved
                .slices()
                .iter()
                .position(|slice| {
                    (slice.first_record..slice.first_record + slice.rows).contains(&record)
                })
                .unwrap()
        };
        assert_eq!((slice_of_record(12), slice_of_record(28)), (1, 3));

        let (exec, drained) = validate(&source, 3).await;
        let error = drained.unwrap_err();
        assert!(
            error.to_string().contains("record 12 has an empty field"),
            "{error}"
        );
        let failed = exec.failed_slices();
        assert_eq!(failed.iter().map(|(slice, _)| *slice).min(), Some(1));
        assert!(
            failed.iter().all(|(slice, _)| [1, 3].contains(slice)),
            "{failed:?}"
        );
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
            let (_, judged) = validate(&source, partitions).await;
            let error = judged.unwrap_err().to_string();
            assert!(
                error.contains("value '20' held by '20' and '20'"),
                "{partitions} partitions: {error}"
            );
        }
    }
}
