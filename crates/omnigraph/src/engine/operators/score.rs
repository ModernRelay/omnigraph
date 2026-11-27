//! `ScoreExec`: the core's exact `Score`, in input order, every other column
//! riding through. No index is read, so the rows are the same under every
//! index state.
//!
//! `distance(rel, $x.f, q)`: every input row leaves with the distance of its
//! vector to the query vector appended under the scored binding's
//! `_distance` column, `F32`; a row whose vector is null drops. The distance
//! is the field's declared one (`l2`, `cosine` or `dot`,
//! [`VectorDistance::evaluate`]); under `cosine` a stored zero vector drops
//! its row as a null does, the row `nearest` leaves out too, and a zero
//! query vector is refused when the plan is bound, before the read runs. A
//! non-finite stored component, query component or result is refused with
//! `E3`.
//!
//! `bm25`: every input row whose identity is among the exact lexical scan's
//! matches leaves with its `bm25_v1` score appended under the binding's
//! `_score` column, `F64`; every other row drops (its value is null,
//! token-empty or does not match). The scan runs when the consumer first
//! polls the operator, before its input is pulled ([`crate::engine::lexical`]),
//! and the input carries no text. The switch the `Score` node declares is a
//! branch here. On the one-pass side the scan scores every matching
//! document as it reads the field, and a row is looked up by its identity.
//! When that match table would pass its limit, a quarter of the plan's
//! memory limit ([`omnigraph_planner::bm25_match_table_bytes`]), and the
//! node declares the `two_pass` fallback, the scan keeps the statistics
//! alone; the input is then gathered into slices ([`rescan_slice_rows`]),
//! each slice's text read again by identity and analyzed, and each row
//! scored from its own text against the complete statistics, the sums the
//! one-pass side computes, in input order. The report's `ran` names the
//! side.

use std::fmt;
use std::sync::Arc;

use crate::engine::lexical::LexicalScore;

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
use omnigraph_compiler::schema::vector::VectorDistance;

use datafusion::physical_plan::metrics::Gauge;
use futures::future::BoxFuture;
use omnigraph_planner::Bm25Pass;

use super::Switch;
use super::external;
use super::memory::WorkMemory;
use super::shape::{BatchSender, Flow, Mapped, Streamed, Streaming, work};
use crate::engine::lexical_scan::Bm25Answer;
use crate::error::{OmniError, Result};

/// The refusal name the pool reports when a scored batch does not fit.
const OUTPUT: &str = "score output";
/// The refusal name the pool reports when the exact lexical scan's scores
/// do not fit.
const LEXICAL_SCAN: &str = "exact lexical scan";
/// The refusal name the pool reports when a two-pass slice does not fit.
const RESCAN: &str = "exact lexical rescan";

/// What one gathered row costs a two-pass slice beside its input row: its
/// identity's entries in the read's `IN` list, the identity list and the
/// sums the read keeps.
const RESCAN_ID_BYTES: u64 = 1024;
/// The fewest and the most input rows a two-pass slice gathers.
const RESCAN_SLICE_FLOOR: u64 = 256;
const RESCAN_SLICE_CEILING: u64 = 16_384;

/// The input rows the two-pass side gathers before it reads their text
/// again, under a query pool of `pool` bytes, the plan's memory limit: the
/// rows the match table's budget holds at [`RESCAN_ID_BYTES`] each (the
/// budget the switch released, [`omnigraph_planner::bm25_match_table_bytes`]),
/// between 256 and 16,384. One read per slice (the last may hold fewer):
/// with no index on the identity column a read scans the identities of the
/// whole table, so the slice bounds the reads, and the gathered rows carry
/// no text.
pub(crate) fn rescan_slice_rows(pool: u64) -> usize {
    let rows = (omnigraph_planner::bm25_match_table_bytes(pool) / RESCAN_ID_BYTES)
        .clamp(RESCAN_SLICE_FLOOR, RESCAN_SLICE_CEILING);
    usize::try_from(rows).unwrap_or(usize::MAX)
}

pub(crate) struct ScoreExec {
    input: Arc<dyn ExecutionPlan>,
    /// The column of the input a score reads: the vector, or under `bm25`
    /// the scored binding's identity.
    column: String,
    /// The appended score column.
    score: String,
    source: ScoreSource,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

/// What a score reads: the bound query vector of a `distance`, or the exact
/// lexical scan's scores, which a row takes by its identity.
#[derive(Clone)]
enum ScoreSource {
    Distance(Arc<Vec<f32>>, VectorDistance),
    /// The scan's answer and the fallback the node declares.
    Bm25(Arc<LexicalScore>, Option<Bm25Pass>),
}

impl ScoreExec {
    /// `bm25`: the rows whose entity, by its identity in `id_column`, is a
    /// matching document of the field corpus `scores` reads, each with its
    /// `bm25_v1` score appended as `score`.
    pub(crate) fn bm25(
        input: Arc<dyn ExecutionPlan>,
        id_column: String,
        score: String,
        scores: Arc<LexicalScore>,
        fallback: Option<Bm25Pass>,
    ) -> Result<Self> {
        if let Some(side @ Bm25Pass::OnePass) = fallback {
            return Err(OmniError::manifest_internal(format!(
                "an exact `bm25` declares the two_pass fallback or none, not {side:?}"
            )));
        }
        let input_schema = input.schema();
        match input_schema
            .field_with_name(&id_column)
            .map(|f| f.data_type())
        {
            Ok(DataType::Utf8) => {}
            _ => {
                return Err(OmniError::manifest_internal(format!(
                    "the exact `bm25` looks rows up by '{id_column}', which its input does not carry as a string"
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
            source: ScoreSource::Bm25(scores, fallback),
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// `distance` of `column` to `query` under `distance`, the field's
    /// declared distance, appended as `score`.
    pub(crate) fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column: String,
        score: String,
        query: Arc<Vec<f32>>,
        distance: VectorDistance,
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
            source: ScoreSource::Distance(query, distance),
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

/// `batch` with each member's `bm25_v1` score appended and every other row
/// removed, each row's score looked up by the identity in its `column`.
fn lexically_scored(
    batch: &RecordBatch,
    column: usize,
    score_of: impl Fn(&str) -> Result<Option<f64>>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let ids = batch
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| OmniError::manifest_internal("the scored identity is not a string"))?;
    let scored = (0..ids.len())
        .map(|row| match ids.is_valid(row) {
            true => score_of(ids.value(row)),
            false => Ok(None),
        })
        .collect::<Result<Vec<_>>>()?;
    let keep = BooleanArray::from_iter(scored.iter().map(|score| Some(score.is_some())));
    let out = Float64Array::from_iter_values(scored.iter().map(|score| score.unwrap_or(0.0)));
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(Arc::new(out));
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

/// The `distance` of each row's vector to `query`, and the mask of the rows
/// whose vector is not null and inside the distance's domain (a `cosine`
/// zero vector is not). The values come from Lance's own batch kernel for
/// the distance (`DistanceType::arrow_batch_func`), the one its flat kNN
/// scores with, so `distance` and an exhaustive `nearest` agree bit for bit
/// and order near ties alike.
fn distances(
    vectors: &FixedSizeListArray,
    query: &[f32],
    distance: VectorDistance,
) -> Result<(Float32Array, BooleanArray)> {
    let values = vectors
        .values()
        .as_any()
        .downcast_ref::<Float32Array>()
        .ok_or_else(|| OmniError::manifest_internal("a vector's components are not F32"))?;
    let width = query.len();
    for row in 0..vectors.len() {
        if vectors.is_null(row) {
            continue;
        }
        let start = vectors.value_offset(row) as usize;
        for i in 0..width {
            if values.is_null(start + i) || !values.value(start + i).is_finite() {
                return Err(non_finite(format!(
                    "a stored vector scored by `distance` holds a non-finite component at {i}"
                )));
            }
        }
    }
    let scored = crate::table_store::metric_type(distance).arrow_batch_func()(
        &Float32Array::from(query.to_vec()),
        vectors,
    )
    .map_err(|error| OmniError::manifest_internal(format!("`distance` kernel: {error}")))?;
    let zero_query = omnigraph_compiler::schema::vector::is_zero_vector(query);
    let mut out = Vec::with_capacity(vectors.len());
    let mut keep = Vec::with_capacity(vectors.len());
    for row in 0..vectors.len() {
        let start = vectors.value_offset(row) as usize;
        let outside = vectors.is_null(row)
            || (distance.refuses_zero_vector()
                && (zero_query
                    || omnigraph_compiler::schema::vector::is_zero_vector(
                        &values.values()[start..start + width],
                    )));
        if outside {
            out.push(0.0);
            keep.push(false);
            continue;
        }
        let value = scored.value(row);
        if !value.is_finite() {
            return Err(non_finite(format!(
                "`distance` ({distance}) over {width} components is not finite"
            )));
        }
        out.push(value);
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
    distance: VectorDistance,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let vectors = batch
        .column(column)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| OmniError::manifest_internal("the scored column is not a vector"))?;
    let (scores, keep) = distances(vectors, query, distance)?;
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
            ScoreSource::Distance(query, distance) => write!(
                f,
                "ScoreExec: {}={distance}({}, query of {})",
                self.score,
                self.column,
                query.len()
            ),
            ScoreSource::Bm25(scores, fallback) => {
                write!(
                    f,
                    "ScoreExec: {}=bm25_v1 ({scores:?}) by {}",
                    self.score, self.column
                )?;
                match fallback {
                    Some(Bm25Pass::TwoPass) => write!(f, ", fallback=two_pass"),
                    Some(Bm25Pass::OnePass) | None => write!(f, ", fallback=none"),
                }
            }
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
                ScoreSource::Distance(query, distance) => Self::try_new(
                    child,
                    self.column.clone(),
                    self.score.clone(),
                    Arc::clone(query),
                    *distance,
                ),
                ScoreSource::Bm25(scores, fallback) => Self::bm25(
                    child,
                    self.column.clone(),
                    self.score.clone(),
                    Arc::clone(scores),
                    *fallback,
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
        if let ScoreSource::Bm25(scores, fallback) = &self.source {
            let memory = work(Arc::clone(&ctx), "ScoreExec", &self.metrics)?;
            let body = Bm25Rows {
                scores: Arc::clone(scores),
                fallback: *fallback,
                ctx: Arc::clone(&ctx),
                metrics: self.metrics.clone(),
                column,
                schema: self.schema(),
                switch: Switch::gauge(&self.metrics),
                pending: Vec::new(),
                pending_rows: 0,
                held: None,
            };
            let input = Arc::clone(&self.input);
            return Ok(self.shape.produce(
                memory,
                &self.metrics,
                move || input.execute(0, ctx),
                body,
            ));
        }
        let input = self.input.execute(0, Arc::clone(&ctx))?;
        let memory = WorkMemory::new(ctx, OUTPUT)?;
        let source = self.source.clone();
        let schema = self.schema();
        Ok(self.shape.map(&self.metrics, input, move |batch| {
            let out = match &source {
                ScoreSource::Distance(query, distance) => {
                    scored(&batch, column, query, *distance, &schema)
                }
                ScoreSource::Bm25(..) => Err(OmniError::manifest_internal(
                    "an exact `bm25` streams through its own body",
                )),
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

/// The per-batch body of an exact `bm25`: `open` runs the corpus pass
/// before the input executes and records the side it left; each input
/// batch is then scored by identity (`one_pass`), or gathered into a slice
/// whose text is read again and analyzed (`two_pass`).
struct Bm25Rows {
    scores: Arc<LexicalScore>,
    fallback: Option<Bm25Pass>,
    ctx: Arc<TaskContext>,
    metrics: ExecutionPlanMetricsSet,
    column: usize,
    schema: SchemaRef,
    switch: Gauge,
    /// The input gathered for the next two-pass slice, held by `held`.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    held: Option<WorkMemory>,
}

impl Bm25Rows {
    /// Send `out`, held under its own lease until the consumer takes it.
    async fn send(
        out: RecordBatch,
        memory: &Arc<WorkMemory>,
        sender: &BatchSender,
    ) -> DfResult<()> {
        let lease = Arc::new(memory.child(OUTPUT)?);
        lease.hold(&out)?;
        sender.send_bounded(out, lease).await
    }

    /// Read the gathered slice's text again, score its rows and send them in
    /// input order.
    async fn flush(&mut self, memory: &Arc<WorkMemory>, sender: &BatchSender) -> DfResult<()> {
        let Some(gathered) = self.held.take() else {
            return Ok(());
        };
        let pending = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        let Bm25Answer::Statistics(scorer) = self.scores.answered().map_err(external)? else {
            return Err(external(OmniError::manifest_internal(
                "an exact `bm25` gathered a slice on its one-pass side",
            )));
        };
        let work = memory.child(RESCAN)?;
        let mut ids: Vec<String> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for batch in &pending {
            let column = batch
                .column(self.column)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    external(OmniError::manifest_internal(
                        "the scored identity is not a string",
                    ))
                })?;
            for id in column.iter().flatten() {
                if seen.insert(id) {
                    ids.push(id.to_string());
                }
            }
        }
        work.entries::<String>(ids.len())?;
        work.string(ids.iter().map(String::len).sum())?;
        drop(seen);
        let totals = self
            .scores
            .rescore(scorer, &ids, &work)
            .await
            .map_err(external)?;
        for batch in pending {
            let out = lexically_scored(
                &batch,
                self.column,
                |id| {
                    totals
                        .get(id)
                        .copied()
                        .map(crate::engine::lexical_scan::checked_score)
                        .transpose()
                },
                &self.schema,
            )
            .map_err(external)?;
            Self::send(out, memory, sender).await?;
        }
        drop((totals, work, gathered));
        Ok(())
    }
}

impl Streamed for Bm25Rows {
    fn open<'a>(&'a mut self, memory: &'a Arc<WorkMemory>) -> BoxFuture<'a, DfResult<bool>> {
        Box::pin(async move {
            let mut scan = WorkMemory::new(Arc::clone(&self.ctx), LEXICAL_SCAN)?;
            scan.set_metrics(self.metrics.clone());
            let table_limit = self
                .fallback
                .map(|_| omnigraph_planner::bm25_match_table_bytes(memory.pool_bytes()));
            self.scores
                .answer(scan, table_limit)
                .await
                .map_err(external)?;
            match self.scores.answered().map_err(external)? {
                Bm25Answer::Scores(_) => Switch::OnePass.record(&self.switch),
                Bm25Answer::Statistics(_) => Switch::TwoPass.record(&self.switch),
            }
            Ok(true)
        })
    }

    fn batch<'a>(
        &'a mut self,
        batch: RecordBatch,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<Flow>> {
        Box::pin(async move {
            if batch.num_rows() == 0 {
                return Ok(Flow::Continue);
            }
            match self.scores.answered().map_err(external)? {
                Bm25Answer::Scores(scores) => {
                    let out =
                        lexically_scored(&batch, self.column, |id| scores.score(id), &self.schema)
                            .map_err(external)?;
                    Self::send(out, memory, sender).await?;
                }
                Bm25Answer::Statistics(_) => {
                    let held = match &self.held {
                        Some(held) => held,
                        None => self.held.insert(memory.child(RESCAN)?),
                    };
                    held.hold(&batch)?;
                    self.pending_rows += batch.num_rows();
                    self.pending.push(batch);
                    if self.pending_rows >= rescan_slice_rows(memory.pool_bytes()) {
                        self.flush(memory, sender).await?;
                    }
                }
            }
            Ok(Flow::Continue)
        })
    }

    fn finish<'a>(
        &'a mut self,
        memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<()>> {
        Box::pin(self.flush(memory, sender))
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
        let (scores, keep) = distances(&rows, &[0.0, 1.0], VectorDistance::L2).unwrap();
        assert_eq!(keep, BooleanArray::from(vec![true, false, true]));
        assert_eq!(scores.value(0), 2.0);
        assert_eq!(scores.value(2), 4.0);
    }

    /// The field's declared distance decides the value: `cosine` is one
    /// minus the cosine and drops a stored zero vector as a null, `dot` is
    /// one minus the dot product and can be negative.
    #[test]
    fn a_distance_is_the_fields_declared_distance_and_cosine_drops_a_zero_vector() {
        let rows = vectors(vec![
            Some(vec![Some(2.0), Some(0.0)]),
            Some(vec![Some(0.0), Some(-0.0)]),
            Some(vec![Some(0.0), Some(4.0)]),
            None,
        ]);
        let (scores, keep) = distances(&rows, &[1.0, 0.0], VectorDistance::Cosine).unwrap();
        assert_eq!(keep, BooleanArray::from(vec![true, false, true, false]));
        assert_eq!(scores.value(0), 0.0);
        assert_eq!(scores.value(2), 1.0);
        let (scores, keep) = distances(&rows, &[1.0, 0.0], VectorDistance::Dot).unwrap();
        assert_eq!(keep, BooleanArray::from(vec![true, true, true, false]));
        assert_eq!(scores.value(0), -1.0);
        assert_eq!(scores.value(1), 1.0);
        assert_eq!(scores.value(2), 1.0);
    }

    #[test]
    fn a_non_finite_component_is_e3() {
        for distance in [
            VectorDistance::L2,
            VectorDistance::Cosine,
            VectorDistance::Dot,
        ] {
            let rows = vectors(vec![Some(vec![Some(f32::NAN), Some(0.0)])]);
            let error = distances(&rows, &[0.0, 1.0], distance).unwrap_err();
            assert!(error.to_string().contains("E3"), "{error}");
        }
        let huge = vectors(vec![Some(vec![Some(f32::MAX), Some(0.0)])]);
        let error = distances(&huge, &[-f32::MAX, 0.0], VectorDistance::L2).unwrap_err();
        assert!(error.to_string().contains("E3"), "{error}");
        let error = distances(&huge, &[f32::MAX, 0.0], VectorDistance::Dot).unwrap_err();
        assert!(error.to_string().contains("E3"), "{error}");
    }
}
