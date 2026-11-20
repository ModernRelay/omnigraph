//! `FuseExec`: `rrf(arm, …)`, the core's `Fuse` (the GQ 3 RFC,
//! section 4, Fusion). Each arm is the plan subtree the same ranking runs
//! standing alone, whose rows arrive in the arm's rank order (a `nearest`
//! window's distance order, an exact score's sort, an ordered read's own
//! order). The arms are drained one after another, in written order; in
//! each, a target takes the next rank at its first row, so fan-out rows of
//! one target add no vote, and contributes `weight / (k + rank)` to its
//! fused score, summed in written order in `F64`. A target an arm did not
//! select takes nothing from it. One row leaves per target any arm
//! selected: the target's columns from the first arm that selected it and
//! the fused score, nothing else. Every arm row and every fused target is
//! charged to the read's work budget, and the held rows to the query's
//! memory pool, so exhaustion refuses the read rather than truncating it.
//!
//! Inside a correlated read whose arms read its outer rows, the fusion is
//! partitioned by the read's tag, the outer row each arm row belongs to:
//! each arm is a correlated read yielding each outer row's rows in its rank
//! order, ranks restart from 1 in every partition, and one row leaves per
//! partition and target, carrying the tag, so the read joins each outer row
//! to its own fused targets.

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch, StringArray, UInt32Array};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::query::value::{DISTANCE_COLUMN, RRF_COLUMN, SCORE_COLUMN};

use super::memory::WorkMemory;
use super::shape::{Breaker, work};
use super::{drain_one, external};
use crate::error::{OmniError, Result};

/// The fusion's constant and weights, resolved from the plan.
#[derive(Debug, Clone)]
pub(crate) struct FuseMethod {
    pub(crate) k: u32,
    /// One weight per arm, in written order.
    pub(crate) weights: Vec<f64>,
}

pub(crate) struct FuseExec {
    arms: Vec<Arc<dyn ExecutionPlan>>,
    method: FuseMethod,
    /// The target's identity column, `<target>.<id>`.
    id_column: String,
    /// The tag column of the enclosing correlated read the fusion is
    /// partitioned by; `None`: one fusion over every arm row.
    partition: Option<String>,
    /// The fused score column appended after the target's columns.
    fused_score: String,
    shape: Breaker,
    metrics: ExecutionPlanMetricsSet,
}

impl FuseExec {
    pub(crate) fn try_new(
        arms: Vec<Arc<dyn ExecutionPlan>>,
        method: FuseMethod,
        target: &str,
        id_column: &str,
        partition: Option<String>,
    ) -> Result<Self> {
        if arms.len() != method.weights.len() || arms.len() < 2 {
            return Err(OmniError::manifest_internal(format!(
                "a fusion of {} arms with {} weights",
                arms.len(),
                method.weights.len()
            )));
        }
        let schemas: Vec<SchemaRef> = arms.iter().map(|arm| arm.schema()).collect();
        let fused_score = format!("{target}.{RRF_COLUMN}");
        let schema = fused_schema(&schemas, target, partition.as_deref(), &fused_score)?;
        Ok(Self {
            arms,
            method,
            id_column: format!("{target}.{id_column}"),
            partition,
            fused_score,
            shape: Breaker::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// The target's columns in the first arm, each nullable where any arm's is,
/// then the partition's tag column when partitioned, then the fused score;
/// every arm carries each target column (and the tag) with the same type. An
/// arm's own score columns stay behind.
fn fused_schema(
    arms: &[SchemaRef],
    target: &str,
    partition: Option<&str>,
    fused_score: &str,
) -> Result<SchemaRef> {
    let prefix = format!("{target}.");
    let scores =
        [DISTANCE_COLUMN, SCORE_COLUMN, RRF_COLUMN].map(|column| format!("{prefix}{column}"));
    let first = arms
        .first()
        .ok_or_else(|| OmniError::manifest_internal("a fusion with no arm"))?;
    let mut fields: Vec<FieldRef> = Vec::new();
    for field in first.fields() {
        if !field.name().starts_with(&prefix) || scores.contains(field.name()) {
            continue;
        }
        let mut nullable = field.is_nullable();
        for arm in &arms[1..] {
            let other = arm.field_with_name(field.name()).map_err(|_| {
                OmniError::manifest_internal(format!(
                    "a fusion arm carries no `{}` column",
                    field.name()
                ))
            })?;
            if other.data_type() != field.data_type() {
                return Err(OmniError::manifest_internal(format!(
                    "fusion arms carry `{}` as {} and {}",
                    field.name(),
                    field.data_type(),
                    other.data_type()
                )));
            }
            nullable |= other.is_nullable();
        }
        fields.push(Arc::new(field.as_ref().clone().with_nullable(nullable)));
    }
    if let Some(tag) = partition {
        for arm in arms {
            let field = arm.field_with_name(tag).map_err(|_| {
                OmniError::manifest_internal(format!(
                    "a partitioned fusion's arm carries no `{tag}` column"
                ))
            })?;
            if field.data_type() != &DataType::UInt32 || field.is_nullable() {
                return Err(OmniError::manifest_internal(format!(
                    "a partitioned fusion's arm carries `{tag}` as {}",
                    field.data_type()
                )));
            }
        }
        fields.push(Arc::new(Field::new(tag, DataType::UInt32, false)));
    }
    fields.push(Arc::new(Field::new(fused_score, DataType::Float64, false)));
    Ok(Arc::new(Schema::new(fields)))
}

impl fmt::Debug for FuseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FuseExec")
            .field("id_column", &self.id_column)
            .field("fused_score", &self.fused_score)
            .field("partition", &self.partition)
            .field("method", &self.method)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for FuseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let weights: Vec<String> = self.method.weights.iter().map(f64::to_string).collect();
        write!(
            f,
            "FuseExec: on={}, score={}, k={}, weights=[{}], arms={}",
            self.id_column,
            self.fused_score,
            self.method.k,
            weights.join(", "),
            self.arms.len()
        )?;
        if let Some(tag) = &self.partition {
            write!(f, ", partition={tag}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for FuseExec {
    fn name(&self) -> &str {
        "FuseExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.arms.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(
            children.len(),
            self.arms.len(),
            "FuseExec keeps its number of arms"
        );
        Ok(Arc::new(Self {
            arms: children,
            method: self.method.clone(),
            id_column: self.id_column.clone(),
            partition: self.partition.clone(),
            fused_score: self.fused_score.clone(),
            shape: Breaker::new(self.schema()),
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "FuseExec has one partition");
        let declared: SchemaRef = self.schema();
        let mut arms = Vec::with_capacity(self.arms.len());
        for arm in &self.arms {
            arms.push((arm.execute(0, Arc::clone(&ctx))?, arm.schema()));
        }
        let method = self.method.clone();
        let id_column = self.id_column.clone();
        let partition = self.partition.clone();
        let memory = work(ctx, "FuseExec", &self.metrics)?;
        Ok(self
            .shape
            .stream(memory, &self.metrics, move |reservation| async move {
                let mut fusion = Fusion::default();
                let mut parts = Vec::with_capacity(arms.len());
                for (index, (stream, schema)) in arms.into_iter().enumerate() {
                    let batch = drain_one(stream, &schema, &reservation).await?;
                    reservation.charge_produced(batch.num_rows() as u64)?;
                    let weight = method.weights[index];
                    let first = fusion
                        .rank_arm(
                            &batch,
                            &id_column,
                            partition.as_deref(),
                            f64::from(method.k),
                            weight,
                            &reservation,
                        )
                        .map_err(external)?;
                    if !first.is_empty() {
                        parts.push(selected(&batch, &first, &declared, &reservation)?);
                    }
                }
                reservation.charge_produced(fusion.scores.len() as u64)?;
                reservation.entries::<f64>(fusion.scores.len())?;
                let mut columns_of: Vec<RecordBatch> = Vec::with_capacity(parts.len());
                let mut offset = 0;
                for part in parts {
                    let rows = part.num_rows();
                    let scores = Float64Array::from(fusion.scores[offset..offset + rows].to_vec());
                    offset += rows;
                    let mut columns = part.columns().to_vec();
                    columns.push(Arc::new(scores) as ArrayRef);
                    columns_of.push(
                        RecordBatch::try_new(Arc::clone(&declared), columns)
                            .map_err(|error| external(OmniError::arrow_internal(error)))?,
                    );
                }
                if columns_of.is_empty() {
                    return Ok(RecordBatch::new_empty(declared));
                }
                reservation.concat(&declared, &columns_of)
            }))
    }
}

/// The running fusion: each partition's target's position in
/// first-selection order and its fused score so far. An unpartitioned
/// fusion is the one partition 0.
#[derive(Default)]
struct Fusion {
    position: HashMap<(u32, String), usize>,
    scores: Vec<f64>,
}

impl Fusion {
    /// Rank `batch`'s targets in arrival order, from 1 in each partition of
    /// the `partition` column (one partition without it), and add each
    /// one's `weight / (k + rank)`; returns the rows at which a target no
    /// earlier arm selected in its partition first appears, in order.
    fn rank_arm(
        &mut self,
        batch: &RecordBatch,
        id_column: &str,
        partition: Option<&str>,
        k: f64,
        weight: f64,
        memory: &WorkMemory,
    ) -> Result<Vec<u32>> {
        let tags = match partition {
            Some(tag) => Some(
                batch
                    .column_by_name(tag)
                    .and_then(|column| column.as_any().downcast_ref::<UInt32Array>())
                    .ok_or_else(|| {
                        OmniError::manifest_internal(format!(
                            "a partitioned fusion arm carries no UInt32 `{tag}` column"
                        ))
                    })?,
            ),
            None => None,
        };
        let ids = batch
            .column_by_name(id_column)
            .and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| {
                OmniError::manifest_internal(format!(
                    "a fusion arm carries no String `{id_column}` column"
                ))
            })?;
        let rows = ids.len();
        memory
            .entries::<(String, usize)>(rows)
            .map_err(|error| memory.error(error))?;
        memory
            .string(ids.get_buffer_memory_size().saturating_mul(2))
            .map_err(|error| memory.error(error))?;
        let mut ranked: HashSet<(u32, &str)> = HashSet::with_capacity(rows);
        let mut ranks: HashMap<u32, u64> = HashMap::new();
        let mut first = Vec::new();
        for row in 0..rows {
            memory.check().map_err(|error| memory.error(error))?;
            if ids.is_null(row) {
                return Err(OmniError::manifest_internal(
                    "a fusion arm row has no target identity",
                ));
            }
            let id = ids.value(row);
            let tag = tags.map_or(0, |tags| tags.value(row));
            if !ranked.insert((tag, id)) {
                continue;
            }
            let rank = ranks.entry(tag).or_default();
            *rank += 1;
            let add = weight / (k + *rank as f64);
            let key = (tag, id.to_string());
            match self.position.get(&key) {
                Some(&at) => self.scores[at] += add,
                None => {
                    self.position.insert(key, self.scores.len());
                    self.scores.push(add);
                    first.push(u32::try_from(row).map_err(|_| {
                        OmniError::manifest_internal("a fusion arm holds more than u32::MAX rows")
                    })?);
                }
            }
        }
        Ok(first)
    }
}

/// The target columns of `batch` at `rows`, under the declared schema less
/// its fused score.
fn selected(
    batch: &RecordBatch,
    rows: &[u32],
    declared: &SchemaRef,
    memory: &WorkMemory,
) -> DfResult<RecordBatch> {
    let indices = UInt32Array::from(rows.to_vec());
    let target_fields: Vec<FieldRef> = declared.fields()[..declared.fields().len() - 1].to_vec();
    let mut columns = Vec::with_capacity(target_fields.len());
    for field in &target_fields {
        let column = batch.column_by_name(field.name()).ok_or_else(|| {
            external(OmniError::manifest_internal(format!(
                "a fusion arm carries no `{}` column",
                field.name()
            )))
        })?;
        columns.push(Arc::clone(column));
    }
    let schema = Arc::new(Schema::new(target_fields));
    let projected = RecordBatch::try_new(schema, columns)
        .map_err(|error| external(OmniError::arrow_internal(error)))?;
    memory.take(&projected, &indices)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arm(ids: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d.__id",
            DataType::Utf8,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(ids.to_vec()))]).unwrap()
    }

    /// Fan-out rows of one target take one rank and add no vote, a target
    /// one arm missed takes nothing from it, and the sum runs in written
    /// order: the first arm's term, then the second's.
    #[test]
    fn each_arm_ranks_its_distinct_targets_once() {
        let (_, ctx) = super::super::fixtures::context(1_048_576, 16);
        let memory = WorkMemory::new(ctx, "fusion test").unwrap();
        let mut fusion = Fusion::default();
        let first = fusion
            .rank_arm(&arm(&["a", "a", "b"]), "d.__id", None, 60.0, 1.0, &memory)
            .unwrap();
        assert_eq!(first, [0, 2]);
        let first = fusion
            .rank_arm(&arm(&["c", "b", "b"]), "d.__id", None, 60.0, 2.0, &memory)
            .unwrap();
        assert_eq!(first, [0]);
        let a = 1.0 / 61.0;
        let b = 1.0 / 62.0 + 2.0 / 62.0;
        let c = 2.0 / 61.0;
        assert_eq!(fusion.scores, [a, b, c]);
    }
}
