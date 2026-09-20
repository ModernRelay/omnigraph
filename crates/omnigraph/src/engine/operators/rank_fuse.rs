//! `RankFuseExec`: `rrf()` as a pipeline breaker over two child plans, the
//! same pipeline lowered under the primary and the secondary arm modes.
//! The body is the copied fusion of `execute_rrf_fusion`: entity ranks from
//! each leg's first-seen row order, `1/(k+rank)` summed, the winners' rows
//! reconstructed in fused order (`build_fused_batch`).

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_schema::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::{breaker_properties, breaker_stream, conform_positional, drain_one, external};
use crate::engine::graph::fuse_arms;
use crate::engine::search::RrfMode;

pub(crate) struct RankFuseExec {
    primary: Arc<dyn ExecutionPlan>,
    secondary: Arc<dyn ExecutionPlan>,
    rrf: RrfMode,
    id_column: String,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl RankFuseExec {
    pub(crate) fn new(
        primary: Arc<dyn ExecutionPlan>,
        secondary: Arc<dyn ExecutionPlan>,
        rrf: RrfMode,
        id_column: String,
    ) -> Self {
        let schema = primary.schema();
        Self {
            primary,
            secondary,
            rrf,
            id_column,
            properties: breaker_properties(schema),
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for RankFuseExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RankFuseExec")
            .field("id_column", &self.id_column)
            .field("k", &self.rrf.k)
            .field("limit", &self.rrf.limit)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for RankFuseExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RankFuseExec: on={}, k={}, limit={}",
            self.id_column, self.rrf.k, self.rrf.limit
        )
    }
}

impl ExecutionPlan for RankFuseExec {
    fn name(&self) -> &str {
        "RankFuseExec"
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.primary, &self.secondary]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        assert_eq!(
            children.len(),
            2,
            "RankFuseExec has a primary and a secondary leg"
        );
        let secondary = children.pop().expect("secondary leg");
        let primary = children.pop().expect("primary leg");
        Ok(Arc::new(Self::new(
            primary,
            secondary,
            self.rrf.clone(),
            self.id_column.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        assert_eq!(partition, 0, "RankFuseExec has one partition");
        let schema: SchemaRef = self.schema();
        let primary_schema = self.primary.schema();
        let secondary_schema = self.secondary.schema();
        let primary = self.primary.execute(0, Arc::clone(&ctx))?;
        let secondary = self.secondary.execute(0, Arc::clone(&ctx))?;
        let rrf = self.rrf.clone();
        let id_column = self.id_column.clone();
        let declared = Arc::clone(&schema);
        Ok(breaker_stream(
            "RankFuseExec",
            schema,
            &ctx,
            &self.metrics,
            move |reservation| async move {
                let primary = drain_one(primary, &primary_schema, &reservation).await?;
                let secondary = drain_one(secondary, &secondary_schema, &reservation).await?;
                reservation
                    .blocking(move |reservation| async move {
                        let fused = fuse_arms(&primary, &secondary, &rrf, &id_column, &reservation)
                            .map_err(external)?;
                        conform_positional(fused, &declared).map_err(external)
                    })
                    .await
            },
        ))
    }
}
