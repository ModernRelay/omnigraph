//! A load's validation: a pipeline breaker that holds the whole delta,
//! judges every check of the plan in one pass over it, and streams the held
//! batches on only when the delta is admitted.

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

use super::memory::WorkMemory;
use super::producer::producer_stream;
use super::{breaker_properties, drain, external, polled};
use crate::db::{Omnigraph, Snapshot};
use crate::loader::LoadMode;
use crate::loader::read_reuse::LoadReadReuse;
use crate::storage_layer::{DeletedIdBudget, WriteBudget};
use crate::validate::load::{LoadAdmission, LoadPhase, int_keyed, judge_load};
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
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl ValidateExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, judge: LoadJudge) -> Self {
        let schema = input.schema();
        Self {
            input,
            judge: Arc::new(judge),
            properties: breaker_properties(schema),
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
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 1, "ValidateExec has one child");
        Ok(Arc::new(Self {
            input: children.remove(0),
            judge: Arc::clone(&self.judge),
            properties: Arc::clone(&self.properties),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ValidateExec has one partition");
        let schema = self.schema();
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let mut work = WorkMemory::new(ctx, "ValidateExec")?;
        work.set_metrics(self.metrics.clone());
        work.metric("input_rows", 0);
        work.metric(VALIDATED_ROWS, 0);
        let judge = Arc::clone(&self.judge);
        let stream = producer_stream(
            schema,
            Arc::new(work),
            Some(&self.metrics),
            move |memory, sender| async move {
                let batches = drain(input, &memory).await?;
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                {
                    let charged = memory.child("load removed and retained rows")?;
                    let mut admit_phase =
                        |phase: LoadPhase| -> crate::error::Result<Box<dyn Send>> {
                            let phase_memory = memory
                                .child("load validation facts")
                                .map_err(|error| memory.error(error))?;
                            let reserve = match phase {
                                LoadPhase::RemovedInput => phase_memory.entries::<&str>(rows),
                                LoadPhase::Extracted => {
                                    admit_validation(&phase_memory, &judge, &batches)
                                }
                            };
                            reserve.map_err(|error| memory.error(error))?;
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
                    judge_load(
                        &judge.table,
                        &judge.checks,
                        &judge.catalog,
                        &judge.snapshot,
                        judge.mode,
                        &batches,
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
                memory.metric(VALIDATED_ROWS, rows);
                for batch in batches {
                    sender.send(batch, Arc::clone(&memory)).await?;
                }
                Ok(())
            },
        );
        Ok(polled(&self.metrics, stream))
    }
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
