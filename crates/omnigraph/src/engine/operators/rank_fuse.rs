//! `RankFuseExec`: `rrf()` as a pipeline breaker over two child plans, the
//! same pipeline lowered under the primary and the secondary arm modes. Each
//! arm is drained and ranked here, by its score, then the fused binding's id,
//! then every other binding's id; the body is the copied fusion of
//! `execute_rrf_fusion`: entity ranks from each leg's row order, `1/(k+rank)`
//! summed, the winners' rows reconstructed in fused order (`build_fused_batch`).

use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ord::sort::{SortColumn, lexsort_to_indices};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion::common::Result as DfResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};

use super::memory::WorkMemory;
use super::{breaker_properties, breaker_stream, conform_positional, drain_one, external, polled};
use crate::engine::graph::fuse_arms;
use crate::engine::search::RrfMode;
use crate::error::OmniError;

/// The order one arm's rows take before fusion: its score column and the
/// direction the index ranks by.
#[derive(Debug, Clone)]
pub(crate) struct ArmOrder {
    pub(crate) score_column: String,
    pub(crate) descending: bool,
}

pub(crate) struct RankFuseExec {
    primary: Arc<dyn ExecutionPlan>,
    secondary: Arc<dyn ExecutionPlan>,
    rrf: RrfMode,
    id_column: String,
    orders: [ArmOrder; 2],
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl RankFuseExec {
    pub(crate) fn new(
        primary: Arc<dyn ExecutionPlan>,
        secondary: Arc<dyn ExecutionPlan>,
        rrf: RrfMode,
        id_column: String,
        orders: [ArmOrder; 2],
    ) -> Self {
        let schema = primary.schema();
        Self {
            primary,
            secondary,
            rrf,
            id_column,
            orders,
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
            "RankFuseExec: on={}, k={}, limit={}, arms=[{} {}, {} {}]",
            self.id_column,
            self.rrf.k,
            self.rrf.limit,
            self.orders[0].score_column,
            direction(self.orders[0].descending),
            self.orders[1].score_column,
            direction(self.orders[1].descending),
        )
    }
}

fn direction(descending: bool) -> &'static str {
    if descending { "desc" } else { "asc" }
}

/// `batch` in the arm's rank order: its score, the fused binding's id, then
/// every other `<binding>.<id>` column by name, nulls last on every key, so
/// equal scores fuse the same way on every run.
fn ranked(
    batch: &RecordBatch,
    order: &ArmOrder,
    id_column: &str,
    memory: &WorkMemory,
) -> DfResult<RecordBatch> {
    let column = |name: &str| {
        batch.column_by_name(name).cloned().ok_or_else(|| {
            external(OmniError::manifest_internal(format!(
                "RRF arm has no column '{name}'"
            )))
        })
    };
    let mut keys = vec![
        SortColumn {
            values: column(&order.score_column)?,
            options: Some(SortOptions {
                descending: order.descending,
                nulls_first: false,
            }),
        },
        SortColumn {
            values: column(id_column)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        },
    ];
    let id_suffix = id_column
        .rfind('.')
        .map(|dot| &id_column[dot..])
        .ok_or_else(|| {
            external(OmniError::manifest_internal(format!(
                "the fused id column '{id_column}' is not a `<binding>.<id>` column"
            )))
        })?;
    let schema = batch.schema();
    let mut others: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| name.ends_with(id_suffix) && *name != id_column)
        .collect();
    others.sort_unstable();
    for name in others {
        keys.push(SortColumn {
            values: column(name)?,
            options: Some(SortOptions {
                descending: false,
                nulls_first: false,
            }),
        });
    }
    memory.entries::<u32>(batch.num_rows())?;
    let indices = lexsort_to_indices(&keys, None)?;
    memory.take(batch, &indices)
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
            self.rrf,
            self.id_column.clone(),
            self.orders.clone(),
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
        let rrf = self.rrf;
        let id_column = self.id_column.clone();
        let orders = self.orders.clone();
        let declared = Arc::clone(&schema);
        let stream = breaker_stream(
            "RankFuseExec",
            schema,
            &ctx,
            &self.metrics,
            move |reservation| async move {
                let primary = drain_one(primary, &primary_schema, &reservation).await?;
                let secondary = drain_one(secondary, &secondary_schema, &reservation).await?;
                reservation
                    .blocking(move |reservation| async move {
                        let primary = ranked(&primary, &orders[0], &id_column, &reservation)?;
                        let secondary = ranked(&secondary, &orders[1], &id_column, &reservation)?;
                        let fused = fuse_arms(&primary, &secondary, &rrf, &id_column, &reservation)
                            .map_err(external)?;
                        conform_positional(fused, &declared).map_err(external)
                    })
                    .await
            },
        );
        Ok(polled(&self.metrics, stream))
    }
}
