//! `TargetCutExec`: a `nearest` window's selection. Its input arrives sorted
//! by the window's score then the ranked binding's identity, so every row of
//! one target is adjacent; the operator keeps rows until the identity
//! changes to a target past the `targets`-th and stops there. Each selected
//! target keeps every row it has (fan-out carries the score and adds no
//! vote), which a row count cannot express.
//!
//! The run reads `selected_targets`, the targets the cut kept, to decide
//! whether the overfetch ladder must widen the window's candidates.

use std::fmt;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::shape::{Mapped, Streaming, empty};

/// The name of the counter of targets a cut kept.
pub(crate) const SELECTED_TARGETS: &str = "selected_targets";

pub(crate) struct TargetCutExec {
    input: Arc<dyn ExecutionPlan>,
    /// The ranked binding's identity column in the input.
    column: String,
    targets: usize,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl TargetCutExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, column: String, targets: usize) -> Self {
        let shape = Streaming::new(input.schema());
        Self {
            input,
            column,
            targets,
            shape,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for TargetCutExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TargetCutExec")
            .field("column", &self.column)
            .field("targets", &self.targets)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for TargetCutExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TargetCutExec: column={} targets={}",
            self.column, self.targets
        )
    }
}

/// The first row of `values` at or after `from` whose value differs from
/// the one before it (`previous` for the first row), with the number of
/// changes seen until `limit` changes are exceeded: `Some(row)` is where the
/// `limit + 1`-th target starts.
fn boundary(
    values: &ArrayRef,
    previous: &mut Option<String>,
    seen: &mut usize,
    limit: usize,
) -> DfResult<Option<usize>> {
    let ids = arrow_cast::cast(values, &arrow_schema::DataType::Utf8)?;
    let ids = ids
        .as_any()
        .downcast_ref::<arrow_array::StringArray>()
        .ok_or_else(|| DataFusionError::Internal("a target identity is not text".to_string()))?;
    for row in 0..ids.len() {
        if ids.is_null(row) {
            return Err(DataFusionError::Internal(
                "a window's target has no identity".to_string(),
            ));
        }
        let id = ids.value(row);
        if previous.as_deref() != Some(id) {
            *seen += 1;
            if *seen > limit {
                *seen = limit;
                return Ok(Some(row));
            }
            *previous = Some(id.to_string());
        }
    }
    Ok(None)
}

impl ExecutionPlan for TargetCutExec {
    fn name(&self) -> &str {
        "TargetCutExec"
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
        assert_eq!(children.len(), 1, "TargetCutExec has one child");
        Ok(Arc::new(Self::new(
            children.pop().expect("one child"),
            self.column.clone(),
            self.targets,
        )))
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "TargetCutExec has one partition");
        let selected: Count = MetricBuilder::new(&self.metrics).counter(SELECTED_TARGETS, 0);
        if self.targets == 0 {
            return Ok(empty(&self.metrics, self.schema()));
        }
        let index = self.input.schema().index_of(&self.column)?;
        let input = self.input.execute(0, ctx)?;
        let limit = self.targets;
        let mut previous: Option<String> = None;
        let mut seen = 0usize;
        Ok(self
            .shape
            .map(&self.metrics, input, move |batch: RecordBatch| {
                let before = seen;
                let cut = boundary(batch.column(index), &mut previous, &mut seen, limit)?;
                selected.add(seen - before);
                Ok(match cut {
                    Some(row) => Mapped::Last(batch.slice(0, row)),
                    None => Mapped::Batch { batch, lease: None },
                })
            }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn ids(values: &[&str]) -> ArrayRef {
        Arc::new(StringArray::from(values.to_vec()))
    }

    #[test]
    fn the_cut_keeps_every_row_of_the_first_targets() {
        let mut previous = None;
        let mut seen = 0;
        // a a b | c: two targets kept, the third starts the cut.
        let cut = boundary(&ids(&["a", "a", "b", "c"]), &mut previous, &mut seen, 2).unwrap();
        assert_eq!((cut, seen), (Some(3), 2));
    }

    #[test]
    fn a_target_continues_across_batches() {
        let mut previous = None;
        let mut seen = 0;
        assert_eq!(
            boundary(&ids(&["a", "b"]), &mut previous, &mut seen, 2).unwrap(),
            None
        );
        // `b` goes on in the next batch; `c` is the third target.
        let cut = boundary(&ids(&["b", "b", "c"]), &mut previous, &mut seen, 2).unwrap();
        assert_eq!((cut, seen), (Some(2), 2));
    }

    #[test]
    fn fewer_targets_than_the_window_pass_whole() {
        let mut previous = None;
        let mut seen = 0;
        let cut = boundary(&ids(&["a", "a"]), &mut previous, &mut seen, 3).unwrap();
        assert_eq!((cut, seen), (None, 1));
    }
}
