//! Retry an underestimated hash build through the dependent ID scan. The
//! probe must still be unpolled: output, probe and scratch failures propagate.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::StreamExt;

use super::streaming_properties;
use crate::error::OmniError;

type Plan = Arc<dyn ExecutionPlan>;

/// Marks the first poll, not stream construction: DataFusion constructs the
/// probe stream before building, then polls it only after the build succeeds.
#[derive(Debug)]
pub(crate) struct HashProbeExec {
    input: Plan,
    started: Arc<AtomicBool>,
}

impl HashProbeExec {
    pub(crate) fn new(input: Plan) -> Self {
        Self {
            input,
            started: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl DisplayAs for HashProbeExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HashProbeExec")
    }
}

impl ExecutionPlan for HashProbeExec {
    fn name(&self) -> &str {
        "HashProbeExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }
    fn children(&self) -> Vec<&Plan> {
        vec![&self.input]
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<Plan>) -> DfResult<Plan> {
        assert_eq!(children.len(), 1, "HashProbeExec has one child");
        Ok(Arc::new(Self::new(children.pop().expect("one child"))))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, ctx)?;
        let started = Arc::clone(&self.started);
        let stream = futures::stream::poll_fn(move |cx| {
            started.store(true, Ordering::Relaxed);
            input.as_mut().poll_next(cx)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

/// Each execution needs its own probe marker and HashJoin build future. Stop
/// rebuilding at the marker so a nested dependent scan keeps its own boundary.
fn fresh_primary(plan: &Plan, started: &Arc<AtomicBool>, fresh: &mut Vec<Plan>) -> DfResult<Plan> {
    if let Some(probe) = (plan.as_ref() as &dyn std::any::Any).downcast_ref::<HashProbeExec>() {
        return Ok(Arc::new(HashProbeExec {
            input: Arc::clone(&probe.input),
            started: Arc::clone(started),
        }));
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(Arc::clone(plan));
    }
    let children = children
        .into_iter()
        .map(|child| fresh_primary(child, started, fresh))
        .collect::<DfResult<_>>()?;
    let plan = Arc::clone(plan).with_new_children(children)?;
    fresh.push(Arc::clone(&plan));
    Ok(plan)
}

#[derive(Debug)]
pub(crate) struct HashFallbackExec {
    primary: Plan,
    lookup: Plan,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl HashFallbackExec {
    pub(crate) fn new(primary: Plan, lookup: Plan) -> Self {
        assert_eq!(
            primary.schema(),
            lookup.schema(),
            "hash and ID lookup schemas agree"
        );
        let properties = streaming_properties(primary.schema());
        Self {
            primary,
            lookup,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl DisplayAs for HashFallbackExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HashFallbackExec: id_lookup on build memory refusal")
    }
}

impl ExecutionPlan for HashFallbackExec {
    fn name(&self) -> &str {
        "HashFallbackExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Plan> {
        vec![&self.primary, &self.lookup]
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn with_new_children(self: Arc<Self>, mut children: Vec<Plan>) -> DfResult<Plan> {
        assert_eq!(children.len(), 2, "HashFallbackExec has two alternatives");
        let lookup = children.pop().expect("two children");
        let primary = children.pop().expect("two children");
        Ok(Arc::new(Self::new(primary, lookup)))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "HashFallbackExec has one partition");
        let started = Arc::new(AtomicBool::new(false));
        let mut fresh = Vec::new();
        let primary = fresh_primary(&self.primary, &started, &mut fresh)?;
        let input = primary.execute(0, Arc::clone(&ctx))?;
        let lookup = Arc::clone(&self.lookup);
        let fallbacks = MetricBuilder::new(&self.metrics).counter("hash_build_fallbacks", 0);
        let stream =
            futures::stream::try_unfold((input, Some(fresh)), move |(mut input, mut primary)| {
                let started = Arc::clone(&started);
                let lookup = Arc::clone(&lookup);
                let ctx = Arc::clone(&ctx);
                let fallbacks = fallbacks.clone();
                async move {
                    loop {
                        match input.next().await {
                            Some(Ok(batch)) => return Ok(Some((batch, (input, primary)))),
                            Some(Err(error)) => {
                                let retry = primary.is_some()
                                    && !started.load(Ordering::Relaxed)
                                    && OmniError::is_query_memory_failure(&error);
                                if let Some(plan) = primary.take() {
                                    crate::instrumentation::record_query_runtime_metrics(&plan);
                                }
                                if !retry {
                                    return Err(error);
                                }
                                drop(input);
                                fallbacks.add(1);
                                input = lookup.execute(0, Arc::clone(&ctx))?;
                            }
                            None => {
                                if let Some(plan) = primary.take() {
                                    crate::instrumentation::record_query_runtime_metrics(&plan);
                                }
                                return Ok(None);
                            }
                        }
                    }
                }
            });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
