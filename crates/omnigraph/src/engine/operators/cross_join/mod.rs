//! `CrossJoinExec`: every pair of a `CrossJoin` node's two inputs that its
//! `filters` hold for, the left collected, each right batch paired through one
//! `PairBuffer`. It knows no text predicate and fills no scan's runtime filter.

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::common::{DataFusionError, Result as DfResult};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::future::BoxFuture;
use omnigraph_compiler::ir::{IRExpr, ParamMap};

use super::memory::WorkMemory;
use super::pair_buffer::PairBuffer;
use super::shape::{BatchSender, CollectedSide, Flow, Streamed, Streaming, work};
use super::{external, joined_schema};

#[derive(Debug)]
pub(crate) struct CrossJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    filters: Vec<IRExpr>,
    params: Arc<ParamMap>,
    shape: Streaming,
    metrics: ExecutionPlanMetricsSet,
}

impl CrossJoinExec {
    pub(crate) fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        filters: Vec<IRExpr>,
        params: Arc<ParamMap>,
    ) -> crate::error::Result<Self> {
        let schema = joined_schema(&left.schema(), &right.schema())?;
        Ok(Self {
            left,
            right,
            filters,
            params,
            shape: Streaming::new(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl DisplayAs for CrossJoinExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.filters.is_empty() {
            return write!(f, "CrossJoinExec");
        }
        let filters: Vec<String> = self.filters.iter().map(ToString::to_string).collect();
        write!(f, "CrossJoinExec: {}", filters.join(" AND "))
    }
}

impl ExecutionPlan for CrossJoinExec {
    fn name(&self) -> &str {
        "CrossJoinExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.shape.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.left, &self.right]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(children.len(), 2, "CrossJoinExec has two children");
        let right = children.pop().expect("right child");
        let left = children.pop().expect("left child");
        Ok(Arc::new(
            Self::try_new(left, right, self.filters.clone(), Arc::clone(&self.params))
                .map_err(external)?,
        ))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "CrossJoinExec has one partition");
        let memory = work(Arc::clone(&ctx), "CrossJoinExec", &self.metrics)?;
        memory.metric("input_rows", 0);
        memory.metric("input_batches", 0);
        let right = Arc::clone(&self.right);
        let body = Pairs {
            left: Arc::clone(&self.left),
            declared: self.schema(),
            filters: self.filters.clone(),
            params: Arc::clone(&self.params),
            buffer: None,
        };
        Ok(self
            .shape
            .produce(memory, &self.metrics, move || right.execute(0, ctx), body))
    }
}

/// The per-batch body over the right side: the left collected before the
/// right executes, each right batch paired through the buffer.
struct Pairs {
    left: Arc<dyn ExecutionPlan>,
    declared: SchemaRef,
    filters: Vec<IRExpr>,
    params: Arc<ParamMap>,
    /// The buffer over the collected left and the reservation holding it.
    buffer: Option<(PairBuffer, WorkMemory)>,
}

impl Streamed for Pairs {
    fn open<'a>(&'a mut self, memory: &'a Arc<WorkMemory>) -> BoxFuture<'a, DfResult<bool>> {
        Box::pin(async move {
            let side = CollectedSide::collect(&self.left, memory, "cross join left").await?;
            if side.batch.num_rows() == 0 {
                return Ok(false);
            }
            let size = memory.ctx.session_config().batch_size();
            let buffer = PairBuffer::new(
                side.batch,
                Arc::clone(&self.declared),
                self.filters.clone(),
                Arc::clone(&self.params),
                size,
                memory,
            )?;
            self.buffer = Some((buffer, side.held));
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
            memory.metric("input_rows", batch.num_rows());
            memory.metric("input_batches", 1);
            if batch.num_rows() == 0 {
                return Ok(Flow::Continue);
            }
            let (buffer, _) = self.buffer.as_mut().ok_or_else(|| {
                DataFusionError::Internal("cross join paired before its left was collected".into())
            })?;
            buffer.start(&batch)?;
            buffer.push_all(sender).await?;
            buffer.flush(sender).await?;
            Ok(Flow::Continue)
        })
    }

    fn finish<'a>(
        &'a mut self,
        _memory: &'a Arc<WorkMemory>,
        sender: &'a BatchSender,
    ) -> BoxFuture<'a, DfResult<()>> {
        Box::pin(async move {
            match self.buffer.as_mut() {
                Some((buffer, _)) => buffer.finish(sender).await,
                None => Ok(()),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::operators::fixtures::{context, texts};
    use futures::StreamExt;

    /// A filterless join of `(mid, number)` matters and `(pid, text)` passage
    /// batches.
    fn join(matters: &[(&str, &str)], passages: &[&[(&str, &str)]]) -> CrossJoinExec {
        CrossJoinExec::try_new(
            texts(["m.mid", "m.number"], &[matters]),
            texts(["p.pid", "p.text"], passages),
            Vec::new(),
            Arc::new(ParamMap::new()),
        )
        .unwrap()
    }

    /// One left row over sixteen one-row right batches under a batch size of
    /// eight: the first output holds the first eight kept rows, not the first
    /// batch's one row nor all sixteen, and the second the other eight.
    #[tokio::test]
    async fn the_first_output_leaves_once_the_kept_rows_reach_the_batch_size() {
        let passages: Vec<[(&str, &str); 1]> = (0..16).map(|_| [("p", "x")]).collect();
        let batches: Vec<&[(&str, &str)]> = passages.iter().map(|batch| &batch[..]).collect();
        let join = join(&[("m", "x")], &batches);
        let (_, ctx) = context(1 << 20, 8);
        let mut stream = join.execute(0, ctx).unwrap();
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 8);
        let rest = datafusion::physical_plan::common::collect(stream)
            .await
            .unwrap();
        let rows: Vec<usize> = rest.iter().map(|batch| batch.num_rows()).collect();
        assert_eq!(rows, [8]);
        let consumed = join
            .metrics()
            .unwrap()
            .sum_by_name("input_batches")
            .map(|value| value.as_usize());
        assert_eq!(consumed, Some(16));
    }
}
