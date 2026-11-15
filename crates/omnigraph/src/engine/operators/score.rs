//! `ScoreExec`: the core's exact `Score`, in input order, every other column
//! riding through. No index is read, so the rows are the same under every
//! index state.
//!
//! `distance(rel, $x.f, q)`: every input row leaves with the distance of its
//! vector to the query vector appended under the scored binding's
//! `_distance` column, `F32`; a row whose vector is null drops. The distance
//! is the field's declared one, `l2` (the sum of squared component
//! differences, no square root), the only one a schema declares in this
//! release. A non-finite stored component, query component or result is
//! refused with `E3`.
//!
//! `bm25` on a field whose index declares an analyzer: every input row whose
//! identity is among the exact lexical scan's matches leaves with its
//! `bm25_v1` score appended under the binding's `_score` column, `F64`;
//! every other row drops (its value is null, token-empty or does not match).

use std::fmt;
use std::sync::Arc;

use std::collections::HashMap;

use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, RecordBatch,
    StringArray,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use omnigraph_compiler::query::codes::E3;
use omnigraph_compiler::query::diagnostic::QueryDiagnostic;

use super::external;
use super::memory::WorkMemory;
use super::shape::{Mapped, Streaming};
use crate::error::{OmniError, Result};

/// The refusal name the pool reports when a scored batch does not fit.
const OUTPUT: &str = "score output";

pub(crate) struct ScoreExec {
    input: Arc<dyn ExecutionPlan>,
    /// The vector column in the input, or under `bm25` its identity column.
    column: String,
    /// The appended score column.
    score: String,
    source: ScoreSource,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

/// What a score reads: the bound query vector of a `distance`, or the exact
/// lexical scan's `bm25_v1` scores by identity.
#[derive(Clone)]
enum ScoreSource {
    Distance(Arc<Vec<f32>>),
    Bm25(Arc<HashMap<String, f64>>),
}

impl ScoreExec {
    /// `bm25` on a field whose index declares an analyzer: the rows whose
    /// identity column `id_column` names a key of `scores`, with its score
    /// appended as `score`.
    pub(crate) fn bm25(
        input: Arc<dyn ExecutionPlan>,
        id_column: String,
        score: String,
        scores: Arc<HashMap<String, f64>>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        match input_schema
            .field_with_name(&id_column)
            .map(|f| f.data_type())
        {
            Ok(DataType::Utf8) => {}
            _ => {
                return Err(OmniError::manifest_internal(format!(
                    "the exact `bm25` reads the identity column '{id_column}', which its input does not carry as a string"
                )));
            }
        }
        let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(&score, DataType::Float64, false)));
        let shape = Streaming::new(Arc::new(Schema::new(fields)));
        Ok(Self {
            input,
            column: id_column,
            score,
            source: ScoreSource::Bm25(scores),
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column: String,
        score: String,
        query: Arc<Vec<f32>>,
    ) -> Result<Self> {
        if let Some(position) = query.iter().position(|value| !value.is_finite()) {
            return Err(non_finite(format!(
                "the query vector of `distance` holds a non-finite component at {position}"
            )));
        }
        let input_schema = input.schema();
        let field = input_schema.field_with_name(&column).map_err(|_| {
            OmniError::manifest_internal(format!(
                "the scored vector column '{column}' is not in the score's input"
            ))
        })?;
        match field.data_type() {
            DataType::FixedSizeList(item, size)
                if item.data_type() == &DataType::Float32
                    && usize::try_from(*size).ok() == Some(query.len()) => {}
            other => {
                return Err(OmniError::manifest_internal(format!(
                    "`distance` scores '{column}' of type {other:?} against a query of {} components",
                    query.len()
                )));
            }
        }
        let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(&score, DataType::Float32, false)));
        let shape = Streaming::new(Arc::new(Schema::new(fields)));
        Ok(Self {
            input,
            column,
            score,
            source: ScoreSource::Distance(query),
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// `batch` with each member's `bm25_v1` score appended and every other row
/// removed.
fn lexically_scored(
    batch: &RecordBatch,
    column: usize,
    scores: &HashMap<String, f64>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let ids = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest_internal("the scored identity is not a string"))?;
    let mut out = Vec::with_capacity(ids.len());
    let mut keep = Vec::with_capacity(ids.len());
    for row in 0..ids.len() {
        match ids
            .is_valid(row)
            .then(|| scores.get(ids.value(row)))
            .flatten()
        {
            Some(score) => {
                out.push(*score);
                keep.push(true);
            }
            None => {
                out.push(0.0);
                keep.push(false);
            }
        }
    }
    let keep = BooleanArray::from(keep);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(Float64Array::from(out)));
    let whole =
        RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)?;
    if keep.true_count() == keep.len() {
        return Ok(whole);
    }
    arrow_select::filter::filter_record_batch(&whole, &keep).map_err(OmniError::arrow_internal)
}

fn non_finite(message: String) -> OmniError {
    OmniError::Compiler(omnigraph_compiler::error::CompilerError::query(
        QueryDiagnostic::execution(E3, message),
    ))
}

/// The squared `l2` distance of each row's vector to `query`, and the mask
/// of the rows whose vector is not null.
fn distances(vectors: &FixedSizeListArray, query: &[f32]) -> Result<(Float32Array, BooleanArray)> {
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| OmniError::manifest_internal("a vector's components are not F32"))?;
    let width = query.len();
    let mut out = Vec::with_capacity(vectors.len());
    let mut keep = Vec::with_capacity(vectors.len());
    for row in 0..vectors.len() {
        if vectors.is_null(row) {
            out.push(0.0);
            keep.push(false);
            continue;
        }
        let start = vectors.value_offset(row) as usize;
        let mut sum = 0.0f32;
        for (i, q) in query.iter().enumerate() {
            let value = values.value(start + i);
            if !value.is_finite() || values.is_null(start + i) {
                return Err(non_finite(format!(
                    "a stored vector scored by `distance` holds a non-finite component at {i}"
                )));
            }
            let difference = value - q;
            sum += difference * difference;
        }
        if !sum.is_finite() {
            return Err(non_finite(format!(
                "`distance` over {width} components is not finite"
            )));
        }
        out.push(sum);
        keep.push(true);
    }
    Ok((Float32Array::from(out), BooleanArray::from(keep)))
}

/// `batch` with the distance column appended and the rows whose vector is
/// null removed.
fn scored(
    batch: &RecordBatch,
    column: usize,
    query: &[f32],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let vectors = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| OmniError::manifest_internal("the scored column is not a vector"))?;
    let (scores, keep) = distances(vectors, query)?;
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(scores));
    let whole =
        RecordBatch::try_new(Arc::clone(schema), columns).map_err(OmniError::arrow_internal)?;
    if keep.true_count() == keep.len() {
        return Ok(whole);
    }
    arrow_select::filter::filter_record_batch(&whole, &keep).map_err(OmniError::arrow_internal)
}

impl fmt::Debug for ScoreExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScoreExec")
            .field("column", &self.column)
            .field("score", &self.score)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for ScoreExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.source {
            ScoreSource::Distance(query) => write!(
                f,
                "ScoreExec: {}=l2({}, query of {})",
                self.score,
                self.column,
                query.len()
            ),
            ScoreSource::Bm25(scores) => write!(
                f,
                "ScoreExec: {}=bm25_v1 of {} matches by {}",
                self.score,
                scores.len(),
                self.column
            ),
        }
    }
}

impl ExecutionPlan for ScoreExec {
    fn name(&self) -> &str {
        "ScoreExec"
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
        assert_eq!(children.len(), 1, "ScoreExec has one child");
        let child = children.pop().expect("one child");
        Ok(Arc::new(
            match &self.source {
                ScoreSource::Distance(query) => Self::try_new(
                    child,
                    self.column.clone(),
                    self.score.clone(),
                    Arc::clone(query),
                ),
                ScoreSource::Bm25(scores) => Self::bm25(
                    child,
                    self.column.clone(),
                    self.score.clone(),
                    Arc::clone(scores),
                ),
            }
            .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "ScoreExec has one partition");
        let column = self.input.schema().index_of(&self.column)?;
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let memory = WorkMemory::new(ctx, OUTPUT)?;
        let source = self.source.clone();
        let schema = self.schema();
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            let out = match &source {
                ScoreSource::Distance(query) => scored(&batch, column, query, &schema),
                ScoreSource::Bm25(scores) => lexically_scored(&batch, column, scores, &schema),
            }
            .map_err(external)?;
            let lease = memory.child(OUTPUT)?;
            lease.hold(&out)?;
            Ok(Mapped::Batch {
                batch: out,
                lease: Some(lease),
            })
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::types::Float32Type;

    fn vectors(rows: Vec<Option<Vec<Option<f32>>>>) -> FixedSizeListArray {
        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(rows, 2)
    }

    #[test]
    fn a_distance_is_the_sum_of_squared_differences_and_a_null_vector_drops() {
        let rows = vectors(vec![
            Some(vec![Some(1.0), Some(0.0)]),
            None,
            Some(vec![Some(0.0), Some(3.0)]),
        ]);
        let (scores, keep) = distances(&rows, &[0.0, 1.0]).unwrap();
        assert_eq!(keep, BooleanArray::from(vec![true, false, true]));
        assert_eq!(scores.value(0), 2.0);
        assert_eq!(scores.value(2), 4.0);
    }

    #[test]
    fn a_non_finite_component_is_e3() {
        let rows = vectors(vec![Some(vec![Some(f32::NAN), Some(0.0)])]);
        let error = distances(&rows, &[0.0, 1.0]).unwrap_err();
        assert!(error.to_string().contains("E3"), "{error}");
        let huge = vectors(vec![Some(vec![Some(f32::MAX), Some(0.0)])]);
        let error = distances(&huge, &[-f32::MAX, 0.0]).unwrap_err();
        assert!(error.to_string().contains("E3"), "{error}");
    }
}
